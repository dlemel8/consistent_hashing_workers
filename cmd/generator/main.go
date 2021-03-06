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
		numberOfJobIds := viper.GetUint32("number_of_job_ids")
		jobIds := generateJobIds(numberOfJobIds)

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

		jobs, err := factory.CreateJobsPublisher()
		if err != nil {
			return errors.Wrap(err, "failed to create jobs publisher")
		}
		defer func() {
			if err := jobs.Close(); err != nil {
				log.WithError(err).Error("failed to close jobs publisher")
			}
		}()

		waitForWorkers()

		return publishJobs(cmd.Context(), jobIds, jobs)
	})
}

func generateJobIds(numberOfJobIds uint32) []uint64 {
	res := make([]uint64, numberOfJobIds)
	for i := uint32(0); i < numberOfJobIds; i++ {
		res[i] = rand.Uint64()
	}
	return res
}

func waitForWorkers() {
	// TODO: do something smarter
	log.Info("wait for workers")
	time.Sleep(5 * time.Second)
}

func publishJobs(ctx context.Context, jobIds []uint64, jobs consistenthashing.Publisher) error {
	numberOfJobs := viper.GetUint32("number_of_jobs")
	minJobDataSize := int(viper.GetSizeInBytes("min_job_data_size"))
	maxJobDataSize := int(viper.GetSizeInBytes("max_job_data_size"))

	log.WithFields(log.Fields{
		"numberOfJobs":   numberOfJobs,
		"minJobDataSize": minJobDataSize,
		"maxJobDataSize": maxJobDataSize,
	}).Info("start publishing jobs")

	for i := uint32(0); i < numberOfJobs; i++ {
		if ctx.Err() != nil {
			log.WithField("published", i).Info("publishing has canceled")
			return nil
		}

		job, err := generateJob(jobIds, minJobDataSize, maxJobDataSize)
		if err != nil {
			return err
		}

		if err := jobs.Publish(fmt.Sprintf("%d", job.Id), job); err != nil {
			return errors.Wrap(err, "failed to publish job")
		}
	}

	log.Info("done publishing jobs")
	return nil
}

func generateJob(jobIds []uint64, minJobDataSize int, maxJobDataSize int) (*consistenthashing.ContinuesJob, error) {
	jobIdIndex := rand.Intn(len(jobIds))
	jobId := jobIds[jobIdIndex]

	dataSize := minJobDataSize + rand.Intn(maxJobDataSize-minJobDataSize)
	data := make([]byte, 0, dataSize)

	if _, err := rand.Read(data); err != nil {
		return nil, errors.Wrap(err, "failed to generate job data")
	}

	return &consistenthashing.ContinuesJob{Id: jobId, Data: data}, nil
}
