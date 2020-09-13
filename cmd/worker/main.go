package main

import (
	"consistenthashing"
	"context"
	"github.com/spf13/viper"
	"log"
)

func main() {
	consistenthashing.SetupConfig()
	rabbitMqUrl := viper.GetString("rabbitmq_url")

	rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection(rabbitMqUrl)
	exitIfError(err, "failed to create RabbitMQ connection")
	defer rabbitmqConnection.Close()

	consumer, err := consistenthashing.CreateRabbitMqConsumer(
		rabbitmqConnection,
		consistenthashing.JobsExchange,
		"jobs",
		"#")
	exitIfError(err, "failed to create RabbitMQ consumer")
	defer consumer.Close()

	jobs, err := consumer.Consume(context.Background(), consistenthashing.ContinuesJob{})
	exitIfError(err, "failed to consume RabbitMQ")

	for job := range jobs {
		log.Printf("got job %s", job)
	}
}

func exitIfError(err error, logMessage string) {
	if err != nil {
		log.Fatalf("%s: %v", logMessage, err)
	}
}
