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
		defer func() {
			if err := factory.Close(); err != nil {
				log.WithError(err).Error("failed to close factory")
			}
		}()

		results, err := factory.CreateResultsPublisher()
		if err != nil {
			return errors.Wrap(err, "failed to create results publisher")
		}
		defer func() {
			if err := results.Close(); err != nil {
				log.WithError(err).Error("failed to close results publisher")
			}
		}()

		consumerId := viper.GetString("hostname")
		jobs, err := factory.CreateJobsConsumer(fmt.Sprintf("jobs_%s", consumerId))
		if err != nil {
			return errors.Wrap(err, "failed to create jobs consumer")
		}
		defer func() {
			if err := jobs.Close(); err != nil {
				log.WithError(err).Error("failed to close jobs consumer")
			}
		}()

		terminate, err := factory.CreateTerminateConsumer()
		if err != nil {
			return errors.Wrap(err, "failed to create terminate consumer")
		}
		defer func() {
			if err := terminate.Close(); err != nil {
				log.WithError(err).Error("failed to close terminate consumer")
			}
		}()

		// TODO - update consume api to return a channel to simplify this flow
		go processJobs(cmd.Context(), consumerId, jobs, results)
		return listenToTerminateSignal(cmd.Context(), terminate)
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

func listenToTerminateSignal(base context.Context, terminate consistenthashing.Consumer) error {
	log.Info("start listening to terminate signal")

	messagePtr := &consistenthashing.TerminateSignal{}
	ctx, cancel := context.WithCancel(base)
	return terminate.Consume(ctx, messagePtr, func() {
		log.Info("got terminate signal")
		cancel()
	})
}
