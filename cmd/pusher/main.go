package main

import (
	"log"

	"task_scheduler/internal/config"
	"task_scheduler/internal/pusher"
)

func main() {

	config, err := config.LoadConfig()

	if err != nil {
		log.Fatal(err)
	}

	pusher := pusher.NewPusher(config)

	if err = pusher.Push(); err != nil {
		log.Fatal(err)
	}
}
