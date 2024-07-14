package scheduler

import (
	"log"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler struct{}

func (h *Handler) Process(message amqp.Delivery) error {
	log.Printf("Received task: %s", message.Body)
	time.Sleep(time.Duration(rand.Intn(30)+10) * time.Second) // Simulate processing time
	log.Printf("Finished task: %s", message.Body)
	return nil
}
