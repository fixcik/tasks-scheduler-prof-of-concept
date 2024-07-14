package config

import (
	"errors"

	"github.com/spf13/viper"
)

type Config struct {
	RabbitMQHost      string `mapstructure:"RABBITMQ_HOST"`
	RabbitMQPort      int    `mapstructure:"RABBITMQ_PORT"`
	RabbitMQUser      string `mapstructure:"RABBITMQ_USER"`
	RabbitMQPass      string `mapstructure:"RABBITMQ_PASS"`
	RabbitMQVHost     string `mapstructure:"RABBITMQ_VHOST"`
	Queue             string `mapstructure:"QUEUE"`
	MaxParallelTasks  int    `mapstructure:"MAX_PARALLEL_TASKS"`
	MAxTasksPerMinute int    `mapstructure:"MAX_TASKS_PER_MINUTE"`
}

func LoadConfig() (*Config, error) {

	viper.SetConfigName("app")
	viper.AddConfigPath(".")
	viper.SetConfigType("env")
	viper.SetConfigFile(".env")
	viper.ReadInConfig()

	viper.SetDefault("RABBITMQ_HOST", "localhost")
	viper.SetDefault("RABBITMQ_PORT", 5672)
	viper.SetDefault("RABBITMQ_USER", "guest")
	viper.SetDefault("RABBITMQ_PASS", "guest")
	viper.SetDefault("RABBITMQ_VHOST", "/")
	viper.SetDefault("MAX_PARALLEL_TASKS", 10)
	viper.SetDefault("MAX_TASKS_PER_MINUTE", 20)

	var config Config

	err := viper.Unmarshal(&config)

	if err != nil {
		return nil, err
	}

	if config.RabbitMQPort == 0 {
		return nil, errors.New("RABBITMQ_PORT environment variable required")
	}

	if config.RabbitMQHost == "" {
		return nil, errors.New("RABBITMQ_HOST environment variable required")
	}

	if config.Queue == "" {
		return nil, errors.New("QUEUE environment variable required")
	}

	return &config, nil
}
