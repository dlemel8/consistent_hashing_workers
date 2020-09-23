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

		rabbitMqUrl := viper.GetString("rabbitmq_url")
		rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection(rabbitMqUrl)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ connection")
		}
		defer rabbitmqConnection.Close()

		waitForWorkers()

		jobs, err := consistenthashing.CreateRabbitMqPublisher(rabbitmqConnection, consistenthashing.JobsExchange)
		if err != nil {
			return errors.Wrap(err, "failed to create RabbitMQ jobs")
		}
		defer jobs.Close()

		numberOfJobs := viper.GetUint32("number_of_jobs")
		return publishJobs(cmd.Context(), jobIds, numberOfJobs, jobs)
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

func publishJobs(
	ctx context.Context,
	jobIds []uint64,
	numberOfJobs uint32,
	jobs *consistenthashing.RabbitMqPublisher) error {

	log.WithField("numberOfJobs", numberOfJobs).Info("start publishing jobs")

	for i := uint32(0); i < numberOfJobs; i++ {
		if ctx.Err() != nil {
			log.WithField("published", i).Info("publishing has canceled")
			return nil
		}

		jobIdIndex := rand.Intn(len(jobIds))
		jobId := jobIds[jobIdIndex]
		job := consistenthashing.ContinuesJob{Id: jobId}
		if err := jobs.Publish(fmt.Sprintf("%d", jobId), job); err != nil {
			return errors.Wrap(err, "failed to publish to RabbitMQ")
		}
	}

	log.WithField("numberOfJobs", numberOfJobs).Info("done publishing jobs")
	return nil
}
