package main

import (
	"consistenthashing"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	consistenthashing.SetupConfig()

	numberOfJobIds := viper.GetUint32("number_of_job_ids")
	jobIds := generateJobIds(numberOfJobIds)

	rabbitMqUrl := viper.GetString("rabbitmq_url")
	rabbitmqConnection, err := consistenthashing.CreateRabbitMqConnection(rabbitMqUrl)
	exitIfError(err, "failed to create RabbitMQ connection")
	defer rabbitmqConnection.Close()

	waitForWorkers()

	publisher, err := consistenthashing.CreateRabbitMqPublisher(rabbitmqConnection, consistenthashing.JobsExchange)
	exitIfError(err, "failed to create RabbitMQ publisher")
	defer publisher.Close()

	numberOfJobs := viper.GetUint32("number_of_jobs")
	publishJobs(jobIds, numberOfJobs, publisher)
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
	time.Sleep(30 * time.Second)
}

func publishJobs(jobIds []uint64, numberOfJobs uint32, publisher *consistenthashing.RabbitMqPublisher) {
	log.WithField("numberOfJobs", numberOfJobs).Info("start publishing jobs")

	for i := uint32(0); i < numberOfJobs; i++ {
		jobIdIndex := rand.Intn(len(jobIds))
		jobId := jobIds[jobIdIndex]
		err := publisher.Publish(fmt.Sprintf("%d", jobId), consistenthashing.ContinuesJob{Id: jobId})
		exitIfError(err, "failed to publish to RabbitMQ")
	}

	log.WithField("numberOfJobs", numberOfJobs).Info("done publishing jobs")
}

func exitIfError(err error, logMessage string) {
	if err != nil {
		log.WithError(err).Fatal(logMessage)
	}
}
