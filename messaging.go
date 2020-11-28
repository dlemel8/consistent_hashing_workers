package consistenthashing

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
)

type MessagingStrategy string

const (
	RabbitMq MessagingStrategy = "RabbitMQ"
	ZeroMq   MessagingStrategy = "ZeroMQ"
)

type Publisher interface {
	Publish(routingKey string, message interface{}) error
	io.Closer
}

type Consumer interface {
	Consume(ctx context.Context, messagePtr interface{}, onNewMessageCallback func()) error
	io.Closer
}

type Factory interface {
	CreateJobsPublisher() (Publisher, error)
	CreateJobsConsumer(consumerId string) (Consumer, error)
	CreateResultsPublisher() (Publisher, error)
	CreateResultsConsumer(consumerId string) (Consumer, error)
	io.Closer
}

func CreateMessagingFactory(strategy MessagingStrategy) (Factory, error) {
	log.WithField("strategy", strategy).Info("creating factory")
	switch strategy {
	case RabbitMq:
		rabbitmqConnection, err := createRabbitMqConnection(viper.GetString("rabbitmq_url"))
		if err != nil {
			return nil, err
		}
		return &RabbitMqFactory{rabbitmqConnection}, nil

	case ZeroMq:
		zeromqContext, err := createZeroMqContext()
		if err != nil {
			return nil, err
		}
		return &ZeroMqFactory{zeromqContext}, nil

	default:
		return nil, fmt.Errorf("unsupported messaging strategy %s", strategy)
	}
}

type RabbitMqFactory struct {
	connection *RabbitMqConnection
}

func (f *RabbitMqFactory) Close() error {
	return f.connection.Close()
}

func (f *RabbitMqFactory) CreateJobsPublisher() (Publisher, error) {
	return createRabbitMqPublisher(f.connection, JobsExchange)
}

func (f *RabbitMqFactory) CreateJobsConsumer(consumerId string) (Consumer, error) {
	queueName := fmt.Sprintf("jobs_%s", consumerId)
	routingKey := fmt.Sprintf("%d", weightPerWorker)
	return createRabbitMqConsumer(f.connection, JobsExchange, queueName, routingKey)
}

func (f *RabbitMqFactory) CreateResultsPublisher() (Publisher, error) {
	return createRabbitMqPublisher(f.connection, JobResultsExchange)
}

func (f *RabbitMqFactory) CreateResultsConsumer(consumerId string) (Consumer, error) {
	queueName := fmt.Sprintf("results_%s", consumerId)
	return createRabbitMqConsumer(f.connection, JobResultsExchange, queueName, "#")
}

type ZeroMqFactory struct {
	context *ZeroMqContext
}

func (f *ZeroMqFactory) Close() error {
	return f.context.Close()
}

func (f *ZeroMqFactory) CreateJobsPublisher() (Publisher, error) {
	endpoint := TcpEndpoint{
		Name:     viper.GetString("zeromq_jobs_endpoint_name"),
		port:     uint16(viper.GetUint32("zeromq_jobs_endpoint_port")),
		isServer: true,
	}

	return createConsistentHashingLoadBalancer(f.context, endpoint)
}

func (f *ZeroMqFactory) CreateJobsConsumer(consumerId string) (Consumer, error) {
	endpoint := TcpEndpoint{
		Name:     viper.GetString("zeromq_jobs_endpoint_name"),
		port:     uint16(viper.GetUint32("zeromq_jobs_endpoint_port")),
		isServer: false,
	}

	return createZeroMqConsumer(f.context, endpoint, jobsAlgorithm)
}

func (f *ZeroMqFactory) CreateResultsPublisher() (Publisher, error) {
	endpoint := TcpEndpoint{
		Name:     viper.GetString("zeromq_results_endpoint_name"),
		port:     uint16(viper.GetUint32("zeromq_results_endpoint_port")),
		isServer: false,
	}
	return createZeroMqPublisher(f.context, endpoint, resultsAlgorithm)
}

func (f *ZeroMqFactory) CreateResultsConsumer(_ string) (Consumer, error) {
	endpoint := TcpEndpoint{
		Name:     viper.GetString("zeromq_results_endpoint_name"),
		port:     uint16(viper.GetUint32("zeromq_results_endpoint_port")),
		isServer: true,
	}
	return createZeroMqConsumer(f.context, endpoint, resultsAlgorithm)
}
