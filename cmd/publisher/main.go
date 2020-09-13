package main

import (
	"consistenthashing"
	"github.com/spf13/viper"
	"log"
)

func main() {
	consistenthashing.SetupConfig()
	rabbitMqUrl := viper.GetString("rabbitmq_url")

	rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection(rabbitMqUrl)
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
