package main

import (
	"consistenthashing"
	"context"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
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

		baseCtx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		consumerId := viper.GetString("hostname")
		jobsCh := make(chan interface{})
		terminateCh := make(chan interface{})

		group, ctx := errgroup.WithContext(baseCtx)
		group.Go(func() error {
			return consumeJobs(ctx, factory, consumerId, jobsCh)
		})
		group.Go(func() error {
			return consumeTerminateSignal(ctx, factory, terminateCh)
		})
		group.Go(func() error {
			defer cancel()
			return processJobs(ctx, consumerId, terminateCh, jobsCh, results)
		})
		return group.Wait()
	})
}

func consumeJobs(ctx context.Context, factory consistenthashing.Factory, consumerId string, messagesCh chan interface{}) error {
	jobs, err := factory.CreateJobsConsumer(fmt.Sprintf("jobs_%s", consumerId))
	if err != nil {
		return errors.Wrap(err, "failed to create jobs consumer")
	}

	defer func() {
		if err := jobs.Close(); err != nil {
			log.WithError(err).Error("failed to close jobs consumer")
		}
	}()
	return jobs.Consume(ctx, &consistenthashing.ContinuesJob{}, messagesCh)
}

func consumeTerminateSignal(ctx context.Context, factory consistenthashing.Factory, terminateCh chan<- interface{}) error {
	terminate, err := factory.CreateTerminateConsumer()
	if err != nil {
		return errors.Wrap(err, "failed to create terminate consumer")
	}

	defer func() {
		if err := terminate.Close(); err != nil {
			log.WithError(err).Error("failed to close terminate consumer")
		}
	}()

	return terminate.Consume(ctx, &consistenthashing.TerminateSignal{}, terminateCh)
}

func processJobs(
	ctx context.Context,
	consumerId string,
	terminateCh <-chan interface{},
	jobsCh <-chan interface{},
	results consistenthashing.Publisher) error {

	log.WithField("consumerId", consumerId).Info("start consuming jobs")
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-terminateCh:
			log.Info("got terminate signal")
			return nil

		case jobObj := <-jobsCh:
			job, ok := jobObj.(*consistenthashing.ContinuesJob)
			if !ok {
				return fmt.Errorf("enexpected job type %#v", jobObj)
			}

			// TODO - move to publisher and get from vyper
			msToSleep := rand.Intn(10)
			time.Sleep(time.Duration(msToSleep) * time.Millisecond)

			result := &consistenthashing.JobResult{Id: job.Id, ProcessedBy: consumerId}
			if err := results.Publish(fmt.Sprintf("%s.%d", consumerId, job.Id), result); err != nil {
				return errors.Wrapf(err, "failed to publish job result %#v", job)
			}
		}
	}
}
