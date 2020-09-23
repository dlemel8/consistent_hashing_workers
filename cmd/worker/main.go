package main

import (
	"consistenthashing"
	"context"
	"fmt"
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

	results, err := consistenthashing.CreateRabbitMqPublisher(rabbitmqConnection, consistenthashing.JobResultsExchange)
	exitIfError(err, "failed to create RabbitMQ results")
	defer results.Close()

	jobs, err := consistenthashing.CreateRabbitMqConsumer(
		rabbitmqConnection,
		consistenthashing.JobsExchange,
		"jobs",
		"#")
	exitIfError(err, "failed to create RabbitMQ jobs")
	defer jobs.Close()

	err = processJobs(jobs, results)
	exitIfError(err, "failed to consume RabbitMQ")
}

func processJobs(jobs *consistenthashing.RabbitMqConsumer, results *consistenthashing.RabbitMqPublisher) error {
	consumerId := viper.GetString("hostname")
	log.WithField("consumerId", consumerId).Info("start consuming jobs")

	messagePtr := &consistenthashing.ContinuesJob{}
	err := jobs.Consume(context.Background(), messagePtr, func() {
		msToSleep := rand.Intn(10)
		time.Sleep(time.Duration(msToSleep) * time.Millisecond)

		result := &consistenthashing.JobResult{Id: messagePtr.Id, ProcessedBy: consumerId}
		if err := results.Publish(fmt.Sprintf("%s.%d", consumerId, messagePtr.Id), result); err != nil {
			log.WithError(err).Error("failed to publish job result")
		}
	})
	return err
}

func exitIfError(err error, logMessage string) {
	if err != nil {
		log.WithError(err).Fatal(logMessage)
	}
}
