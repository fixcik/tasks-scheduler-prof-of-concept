package mq

import (
	"fmt"
	"task_scheduler/internal/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

func makeMqUrl(config *config.Config) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d%s", config.RabbitMQUser, config.RabbitMQUser, config.RabbitMQHost, config.RabbitMQPort, config.RabbitMQVHost)
}

func Setup(config *config.Config) (*amqp.Channel, *amqp.Connection, error) {
	conn, err := amqp.Dial(makeMqUrl(config))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	var args = make(amqp.Table)
	args["x-max-priority"] = 2

	_, err = ch.QueueDeclare(config.Queue, true, false, false, false, args)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return ch, conn, nil
}
