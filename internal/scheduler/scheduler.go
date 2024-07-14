package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"task_scheduler/internal/config"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/time/rate"
)

type Scheduler struct {
	config *config.Config
	pool   *sync.Pool
}

func NewScheduler(config *config.Config) *Scheduler {
	return &Scheduler{
		config: config,
		pool: &sync.Pool{
			New: func() interface{} {
				return new(Handler)
			},
		},
	}
}

func (s *Scheduler) setupRabbitMQ() (*amqp.Channel, *amqp.Connection, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d%s", s.config.RabbitMQUser, s.config.RabbitMQUser, s.config.RabbitMQHost, s.config.RabbitMQPort, s.config.RabbitMQVHost))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	_, err = ch.QueueDeclare(s.config.Queue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return ch, conn, nil
}

func (s *Scheduler) consumeMessages(ch *amqp.Channel, ctx context.Context, wg *sync.WaitGroup, limiter *rate.Limiter, parallelTasks, waitingTasks *atomic.Int32) error {
	msgs, err := ch.Consume(
		s.config.Queue,
		"",
		false,
		false,
		false,
		false,
		amqp.Table{
			"prefetch-count": s.config.MaxParallelTasks,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	for i := 0; i < s.config.MaxParallelTasks; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case d, ok := <-msgs:
					if !ok {
						return
					}
					waitingTasks.Add(1)
					err = limiter.Wait(ctx)
					waitingTasks.Add(-1)
					if err != nil {
						log.Printf("failed to wait for rate limiter: %v", err)
						continue
					}
					parallelTasks.Add(1)
					handler := s.pool.Get().(*Handler)
					err = handler.Process(d)
					if err != nil {
						d.Nack(false, true)
					} else {
						d.Ack(false)
					}
					parallelTasks.Add(-1)
					s.pool.Put(handler)
				}

			}
		}()
	}

	return nil
}

func (s *Scheduler) Consume() error {
	ch, conn, err := s.setupRabbitMQ()
	if err != nil {
		return err
	}
	defer func() {
		ch.Close()
		conn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(s.config.MaxParallelTasks)

	limiter := rate.NewLimiter(rate.Every(time.Minute/time.Duration(s.config.MaxTasksPerMinute)), s.config.MaxTasksPerMinute)

	parallelTasks := atomic.Int32{}
	waitingTasks := atomic.Int32{}

	go s.PrintInto(ctx, limiter, &parallelTasks, &waitingTasks)

	err = s.consumeMessages(ch, ctx, &wg, limiter, &parallelTasks, &waitingTasks)
	if err != nil {
		return err
	}

	wg.Wait()

	return nil
}

func (s *Scheduler) PrintInto(ctx context.Context, limiter *rate.Limiter, parallelTasks, waitingTasks *atomic.Int32) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			log.Printf("Free capacity: %f, waiting tasks: %d, parallel tasks: %d\n", limiter.Tokens(), waitingTasks.Load(), parallelTasks.Load())
		}
	}
}
