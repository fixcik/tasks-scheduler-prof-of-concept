package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
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

func (s *Scheduler) Consume() error {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d%s", s.config.RabbitMQUser, s.config.RabbitMQUser, s.config.RabbitMQHost, s.config.RabbitMQPort, s.config.RabbitMQVHost))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
		return err
	}
	defer ch.Close()

	ch.QueueDeclare(s.config.Queue, true, false, false, false, nil)

	msgs, err := ch.Consume(
		s.config.Queue,
		"",
		false,
		false,
		false,
		false,
		amqp.Table{
			"prefetch-count": 10,
		},
	)

	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
		return err
	}

	var wg sync.WaitGroup
	wg.Add(s.config.MaxParallelTasks)

	limiter := rate.NewLimiter(rate.Every(time.Minute/time.Duration(s.config.MAxTasksPerMinute)), s.config.MAxTasksPerMinute)

	parallel_tasks := 0

	go func() {
		for {
			time.Sleep(time.Second)
			log.Printf("Tokens: %f, parallel tasks: %d\n", limiter.Tokens(), parallel_tasks)
		}
	}()

	for i := 0; i < s.config.MaxParallelTasks; i++ {
		go func(i int) {
			defer wg.Done()
			for d := range msgs {
				err = limiter.Wait(context.Background())
				if err != nil {
					log.Printf("Error waiting for rate limiter: %s", err)
					d.Nack(false, true)
					continue
				}
				parallel_tasks++
				handler := s.pool.Get().(*Handler)
				err = handler.Process(d)
				if err != nil {
					d.Nack(false, true)
				} else {
					d.Ack(false)
				}
				parallel_tasks--
				s.pool.Put(handler)

			}
		}(i)
	}

	wg.Wait()

	return nil
}
