package main

import (
	"log"

	"task_scheduler/internal/config"
	"task_scheduler/internal/publisher"
)

func main() {

	config, err := config.LoadConfig()

	if err != nil {
		log.Fatal(err)
	}

	pusher := publisher.NewPublisher(config)

	if err = pusher.Push(); err != nil {
		log.Fatal(err)
	}
}
