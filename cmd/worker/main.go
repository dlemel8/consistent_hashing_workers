package main

import (
	"consistenthashing"
	"context"
	"log"
)

func main() {
	// TODO - extract url to config
	rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection("amqp://test:test123@localhost:5672/")
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
