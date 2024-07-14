package pusher

import (
	"fmt"
	"log"
	"task_scheduler/internal/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Pusher struct {
	config *config.Config
}

func NewPusher(config *config.Config) *Pusher {
	return &Pusher{
		config: config,
	}
}

func (p *Pusher) Push() error {

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d%s", p.config.RabbitMQUser, p.config.RabbitMQUser, p.config.RabbitMQHost, p.config.RabbitMQPort, p.config.RabbitMQVHost))
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

	_, err = ch.QueueDeclare(p.config.Queue, true, false, false, false, nil)

	if err != nil {
		return err
	}

	for i := 0; i < 100; i++ {
		err = ch.Publish(
			"",
			p.config.Queue,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(fmt.Sprintf("%d message", i)),
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}
