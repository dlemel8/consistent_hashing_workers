package main

import (
	"consistenthashing"
	"log"
)

func main() {
	// TODO - extract url to config
	rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection("amqp://test:test123@localhost:5672/")
	exitIfError(err, "failed to create RabbitMQ connection")
	defer rabbitmqConnection.Close()

	publisher, err := consistenthashing.CreateRabbitMqPublisher(rabbitmqConnection, consistenthashing.JobsExchange)
	exitIfError(err, "failed to create RabbitMQ publisher")
	defer publisher.Close()

	err = publisher.Publish("bla", consistenthashing.ContinuesJob{Id: 1234})
	exitIfError(err, "failed to publish to RabbitMQ")
}

func exitIfError(err error, logMessage string) {
	if err != nil {
		log.Fatal(logMessage)
	}
}
