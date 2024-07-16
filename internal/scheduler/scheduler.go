package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"task_scheduler/internal/config"
	"task_scheduler/internal/mq"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/time/rate"
)

type Scheduler struct {
	config *config.Config
	pool   *sync.Pool
	ctx    context.Context
}

func NewScheduler(ctx context.Context, config *config.Config) *Scheduler {
	return &Scheduler{
		config: config,
		ctx:    ctx,
		pool: &sync.Pool{
			New: func() interface{} {
				return NewHandler(ctx)
			},
		},
	}
}

func (s *Scheduler) consumeMessages(ch *amqp.Channel, ctx context.Context, wg *sync.WaitGroup, limiter *rate.Limiter) error {
	ch.Qos(s.config.MaxParallelTasks, 0, false)
	msgs, err := ch.Consume(
		s.config.Queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	var idleGoroutines, parallelTasks, waitingTasks atomic.Int32
	idleGoroutines.Store(int32(s.config.MaxParallelTasks))

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 5):
				log.Printf(
					"Free capacity: %f, waiting tasks: %d, parallel tasks: %d, idle goroutines: %d",
					limiter.Tokens(), waitingTasks.Load(), parallelTasks.Load(), idleGoroutines.Load(),
				)
			}
		}
	}()

	for i := 0; i < s.config.MaxParallelTasks; i++ {
		go func() {
			defer wg.Done()
			idle := true
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond * 100):
					if !idle {
						idleGoroutines.Add(1)
						idle = true
					}
				case d, ok := <-msgs:
					if !ok {
						return
					}
					if idleGoroutines.Load() == int32(s.config.MaxParallelTasks) {
						// Reserve tokens for smooth start handle tasks
						limiter.ReserveN(time.Now(), int(limiter.Tokens()))
					}
					if idle {
						idleGoroutines.Add(-1)
						idle = false
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
						d.Nack(false, false)
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
	ch, conn, err := mq.Setup(s.config)
	if err != nil {
		return err
	}
	defer func() {
		ch.Close()
		conn.Close()
	}()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(s.config.MaxParallelTasks)

	limiter := rate.NewLimiter(rate.Every(time.Minute/time.Duration(s.config.MaxTasksPerMinute)), s.config.MaxTasksPerMinute)

	err = s.consumeMessages(ch, ctx, &wg, limiter)
	if err != nil {
		return err
	}

	wg.Wait()

	return nil
}
