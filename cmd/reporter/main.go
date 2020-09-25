package main

import (
	"consistenthashing"
	"context"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
)

type resultsReport struct {
	expectedResults    uint32
	processedResults   uint32
	jobIdToProcessedBy map[uint64][]string
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
		if len(processedBy) > 1 {
			res++
		}
	}
	return res
}

func main() {
	consistenthashing.RunCommand(func(cmd *cobra.Command, args []string) error {
		rabbitMqUrl := viper.GetString("rabbitmq_url")
		rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection(rabbitMqUrl)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ connection")
		}
		defer rabbitmqConnection.Close()

		results, err := consistenthashing.CreateRabbitMqConsumer(
			rabbitmqConnection,
			consistenthashing.JobResultsExchange,
			"results",
			"#",
		)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ results consumer")
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

func processResults(base context.Context, results *consistenthashing.RabbitMqConsumer) (*resultsReport, error) {
	res := &resultsReport{
		expectedResults:    viper.GetUint32("number_of_jobs"),
		jobIdToProcessedBy: make(map[uint64][]string),
	}

	log.WithField("numberOfJobs", res.expectedResults).Info("start consuming results")

	messagePtr := &consistenthashing.JobResult{}
	ctx, cancel := context.WithCancel(base)
	err := results.Consume(ctx, messagePtr, func() {
		res.processedResults++

		appendToSliceValue(res.jobIdToProcessedBy, messagePtr.Id, messagePtr.ProcessedBy)

		if res.doneProcessing() {
			cancel()
		}
	})

	if err != nil {
		return nil, errors.Wrap(err, "failed to consume RabbitMQ")
	}

	return res, nil
}

func appendToSliceValue(mapOfSlices map[uint64][]string, key uint64, value string) {
	if _, ok := mapOfSlices[key]; !ok {
		mapOfSlices[key] = make([]string, 0)
	}
	mapOfSlices[key] = append(mapOfSlices[key], value)
}
