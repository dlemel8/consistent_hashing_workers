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
		strategy := consistenthashing.MessagingStrategy(viper.GetString("messaging_strategy"))
		factory, err := consistenthashing.CreateMessagingFactory(strategy)
		if err != nil {
			return errors.Wrap(err, "failed to create messaging factory")
		}
		defer factory.Close()

		results, err := factory.CreateResultsPublisher()
		if err != nil {
			return errors.Wrap(err, "failed to create results publisher")
		}
		defer results.Close()

		consumerId := viper.GetString("hostname")
		jobs, err := factory.CreateJobsConsumer(fmt.Sprintf("jobs_%s", consumerId))
		if err != nil {
			return errors.Wrap(err, "failed to create jobs consumer")
		}
		defer jobs.Close()

		return processJobs(cmd.Context(), consumerId, jobs, results)
	})
}

func processJobs(
	ctx context.Context,
	consumerId string,
	jobs consistenthashing.Consumer,
	results consistenthashing.Publisher) error {

	log.WithField("consumerId", consumerId).Info("start consuming jobs")

	messagePtr := &consistenthashing.ContinuesJob{}
	err := jobs.Consume(ctx, messagePtr, func() {
		// TODO - move to publisher and get from vyper
		msToSleep := rand.Intn(10)
		time.Sleep(time.Duration(msToSleep) * time.Millisecond)

		result := &consistenthashing.JobResult{Id: messagePtr.Id, ProcessedBy: consumerId}
		if err := results.Publish(fmt.Sprintf("%s.%d", consumerId, messagePtr.Id), result); err != nil {
			log.WithError(err).Error("failed to publish job result")
		}
	})
	return errors.Wrap(err, "failed to consume jobs")
}
