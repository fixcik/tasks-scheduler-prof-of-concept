package scheduler

import (
	"context"
	"log"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Handler struct {
	ctx context.Context
}

func NewHandler(ctx context.Context) *Handler {
	return &Handler{ctx: ctx}
}

func (h *Handler) Process(message amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(h.ctx, 40*time.Second)
	defer cancel()

	log.Printf("Run task: %s", message.Body)
	now := time.Now()
	select {
	case <-time.After(time.Duration(rand.Intn(50)+10) * time.Second):
		log.Printf("Finished task: %s at %s", message.Body, time.Since(now).String())
	case <-ctx.Done():
		log.Printf("Task timeout: %s", message.Body)
		// TODO: mark as long task
		return ctx.Err()
	}

	return nil
}
