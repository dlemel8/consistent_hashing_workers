package main

import (
	"consistenthashing"
	"context"
	"fmt"
	"github.com/deckarep/golang-set"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"strings"
	"time"
)

type resultsReport struct {
	expectedResults    uint32
	processedResults   uint32
	jobIdToProcessedBy map[uint64]mapset.Set
}

func (r *resultsReport) processed(id uint64, processedBy string) {
	r.processedResults++

	if _, ok := r.jobIdToProcessedBy[id]; !ok {
		r.jobIdToProcessedBy[id] = mapset.NewSet()
	}
	r.jobIdToProcessedBy[id].Add(processedBy)
}

func (r *resultsReport) doneProcessing() bool {
	return r.processedResults >= r.expectedResults
}

func (r *resultsReport) String() string {
	lines := []string{
		"Job Results Report",
		"##################",
		fmt.Sprintf("Done procecing %d/%d jobs", r.processedResults, r.expectedResults),
		fmt.Sprintf("Seen %d job ids", len(r.jobIdToProcessedBy)),
		fmt.Sprintf("Seen %d job ids processed by more than one worker", r.processedByMoreThanOneWorker()),
	}
	return strings.Join(lines, "\n") + "\n"
}

func (r *resultsReport) processedByMoreThanOneWorker() uint32 {
	res := uint32(0)
	for _, processedBy := range r.jobIdToProcessedBy {
		if processedBy.Cardinality() > 1 {
			res++
		}
	}
	return res
}

func main() {
	consistenthashing.RunCommand(func(cmd *cobra.Command, args []string) error {
		strategy := consistenthashing.MessagingStrategy(viper.GetString("messaging_strategy"))
		factory, err := consistenthashing.CreateMessagingFactory(strategy)
		if err != nil {
			return errors.Wrap(err, "failed to create messaging factory")
		}
		defer func() {
			if err := factory.Close(); err != nil {
				log.WithError(err).Errorf("failed to close factory")
			}
		}()

		terminate, err := factory.CreateTerminatePublisher()
		if err != nil {
			return errors.Wrap(err, "failed to create terminate publisher")
		}
		defer func() {
			if err := terminate.Close(); err != nil {
				log.WithError(err).Error("failed to close terminate consumer")
			}
		}()

		baseCtx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
		defer cancel()

		resultsCh := make(chan interface{})
		group, ctx := errgroup.WithContext(baseCtx)
		group.Go(func() error {
			return consumeResults(ctx, factory, resultsCh)
		})
		group.Go(func() error {
			defer cancel()

			report, err := processResults(ctx, resultsCh)
			if err != nil {
				return err
			}

			return saveReport(report)
		})
		res := group.Wait()

		log.Info("publishing terminate signal")
		if err := terminate.Publish("", consistenthashing.TerminateSignal{}); err != nil {
			log.WithError(err).Error("failed to publish terminate message")
		}

		return res
	})
}

func consumeResults(ctx context.Context, factory consistenthashing.Factory, resultsCh chan<- interface{}) error {
	results, err := factory.CreateResultsConsumer()
	if err != nil {
		return errors.Wrap(err, "failed to create results consumer")
	}

	defer func() {
		if err := results.Close(); err != nil {
			log.WithError(err).Error("failed to close results consumer")
		}
	}()

	return results.Consume(ctx, resultsCh, func() interface{} {
		return &consistenthashing.JobResult{}
	})
}

func processResults(ctx context.Context, resultsCh <-chan interface{}) (*resultsReport, error) {
	res := &resultsReport{
		expectedResults:    viper.GetUint32("number_of_jobs"),
		jobIdToProcessedBy: make(map[uint64]mapset.Set),
	}

	log.WithField("numberOfJobs", res.expectedResults).Info("start consuming results")

	for {
		select {
		case <-ctx.Done():
			return res, nil

		case resultObj := <-resultsCh:
			result, ok := resultObj.(*consistenthashing.JobResult)
			if !ok {
				return nil, fmt.Errorf("enexpected message type %T", resultObj)
			}
			res.processed(result.Id, result.ProcessedBy)

			if res.processedResults%1000 == 0 {
				log.WithField("processed", res.processedResults).Info("status")
			}

			if res.doneProcessing() {
				return res, nil
			}
		}
	}
}

func saveReport(report *resultsReport) error {
	reportPath := viper.GetString("report_path")
	log.WithField("path", reportPath).Info("saving report")
	if err := ioutil.WriteFile(reportPath, []byte(report.String()), 0644); err != nil {
		return errors.Wrap(err, "failed to save reports")
	}
	return nil
}
