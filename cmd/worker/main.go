package main

import (
	"consistenthashing"
	"context"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"math/rand"
	"time"
)

func main() {
	consistenthashing.RunCommand(func(cmd *cobra.Command, args []string) error {
		rabbitMqUrl := viper.GetString("rabbitmq_url")
		rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection(rabbitMqUrl)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ connection")
		}
		defer rabbitmqConnection.Close()

		results, err := consistenthashing.CreateRabbitMqPublisher(rabbitmqConnection, consistenthashing.JobResultsExchange)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ results publisher")
		}
		defer results.Close()

		consumerId := viper.GetString("hostname")
		jobs, err := consistenthashing.CreateRabbitMqConsumer(
			rabbitmqConnection,
			consistenthashing.JobsExchange,
			fmt.Sprintf("jobs_%s", consumerId),
			"8",
		)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ jobs consumer")
		}
		defer jobs.Close()

		return processJobs(cmd.Context(), consumerId, jobs, results)
	})
}

func processJobs(
	ctx context.Context,
	consumerId string,
	jobs *consistenthashing.RabbitMqConsumer,
	results *consistenthashing.RabbitMqPublisher) error {

	log.WithField("consumerId", consumerId).Info("start consuming jobs")

	messagePtr := &consistenthashing.ContinuesJob{}
	err := jobs.Consume(ctx, messagePtr, func() {
		msToSleep := rand.Intn(10)
		time.Sleep(time.Duration(msToSleep) * time.Millisecond)

		result := &consistenthashing.JobResult{Id: messagePtr.Id, ProcessedBy: consumerId}
		if err := results.Publish(fmt.Sprintf("%s.%d", consumerId, messagePtr.Id), result); err != nil {
			log.WithError(err).Error("failed to publish job result")
		}
	})
	return errors.Wrap(err, "failed to consume RabbitMQ")
}
