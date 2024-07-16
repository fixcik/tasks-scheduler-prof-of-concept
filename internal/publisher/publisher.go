package publisher

import (
	"fmt"
	"math/rand"
	"task_scheduler/internal/config"
	"task_scheduler/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	config *config.Config
}

func NewPublisher(config *config.Config) *Publisher {
	return &Publisher{
		config: config,
	}
}

func (p *Publisher) Push() error {
	ch, conn, err := mq.Setup(p.config)
	if err != nil {
		return err
	}
	defer func() {
		ch.Close()
		conn.Close()
	}()

	for i := 0; i < 100; i++ {
		priority := uint8(rand.Intn(2))

		taskBody := fmt.Sprintf("%d task", i)
		if priority == 0 {
			taskBody += " (slow)"
		}

		err = ch.Publish(
			"",
			p.config.Queue,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Priority:    priority,
				Body:        []byte(taskBody),
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}
