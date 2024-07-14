package main

import (
	"log"

	"task_scheduler/internal/config"
	scheduler "task_scheduler/internal/scheduler"
)

func main() {
	config, err := config.LoadConfig()

	if err != nil {
		log.Fatal(err)
	}

	scheduler := scheduler.NewScheduler(config)
	error := scheduler.Consume()

	if error != nil {
		log.Fatal(error)
	}
}
