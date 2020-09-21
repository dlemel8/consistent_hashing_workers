package main

import (
	"consistenthashing"
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
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

	err = processJobs(err, consumer)
	exitIfError(err, "failed to consume RabbitMQ")
}

func processJobs(err error, consumer *consistenthashing.RabbitMqConsumer) error {
	messagePtr := &consistenthashing.ContinuesJob{}
	err = consumer.Consume(context.Background(), messagePtr, func() {
		msToSleep := rand.Intn(10)
		time.Sleep(time.Duration(msToSleep) * time.Millisecond)
		log.WithFields(log.Fields{
			"jobId":     messagePtr.Id,
			"msToSleep": msToSleep,
		}).Info("done processing job")
	})
	return err
}

func exitIfError(err error, logMessage string) {
	if err != nil {
		log.WithError(err).Fatal(logMessage)
	}
}
