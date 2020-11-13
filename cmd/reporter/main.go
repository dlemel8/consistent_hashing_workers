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
	"strings"
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
	return strings.Join(lines, "\n")
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
		defer factory.Close()

		results, err := factory.CreateResultsConsumer("results")
		if err != nil {
			return errors.Wrap(err, "failed to create results consumer")
		}
		defer results.Close()

		report, err := processResults(cmd.Context(), results)
		if err != nil {
			return err
		}
		fmt.Println(report)

		return nil
	})
}

func processResults(base context.Context, results consistenthashing.Consumer) (*resultsReport, error) {
	res := &resultsReport{
		expectedResults:    viper.GetUint32("number_of_jobs"),
		jobIdToProcessedBy: make(map[uint64]mapset.Set),
	}

	log.WithField("numberOfJobs", res.expectedResults).Info("start consuming results")

	messagePtr := &consistenthashing.JobResult{}
	ctx, cancel := context.WithCancel(base)
	err := results.Consume(ctx, messagePtr, func() {
		res.processed(messagePtr.Id, messagePtr.ProcessedBy)
		if res.doneProcessing() {
			cancel()
		}
	})

	if err != nil {
		return nil, errors.Wrap(err, "failed to consume results")
	}

	return res, nil
}
