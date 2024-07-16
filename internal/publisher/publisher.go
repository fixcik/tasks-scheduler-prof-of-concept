package publisher

import (
	"fmt"
	"math/rand"
	"strconv"
	"task_scheduler/internal/config"
	"task_scheduler/internal/mq"
	"time"

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

	now := time.Now()
	endOfDay := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, now.Location())

	// publish 1000 messages with random priority (1 or 2) and random body.
	for i := 0; i < 1000; i++ {
		priority := uint8(rand.Intn(2)) + 1

		taskBody := fmt.Sprintf("%d task", i)
		if priority == 1 {
			taskBody += " (slow)"
		}

		err = ch.Publish(
			"",
			p.config.Queue,
			false,
			false,
			amqp.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp.Persistent,
				Priority:     priority,
				Body:         []byte(taskBody),
				Expiration:   strconv.FormatInt(endOfDay.Sub(now).Milliseconds(), 10),
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}
