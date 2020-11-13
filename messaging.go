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
		rabbitmqConnection, err := CreateRabbitMqConnection(viper.GetString("rabbitmq_url"))
		if err != nil {
			return nil, err
		}
		return &RabbitMqFactory{rabbitmqConnection}, nil

	case ZeroMq:
		zeromqContext, err := CreateZeroMqContext()
		if err != nil {
			return nil, err
		}
		return &ZeroMqFactory{zeromqContext}, nil

	default:
		return nil, fmt.Errorf("unsupported messaging strategy %d", strategy)
	}
}

type RabbitMqFactory struct {
	connection *RabbitMqConnection
}

func (f *RabbitMqFactory) Close() error {
	return f.connection.Close()
}

func (f *RabbitMqFactory) CreateJobsPublisher() (Publisher, error) {
	return CreateRabbitMqPublisher(f.connection, JobsExchange)
}

func (f *RabbitMqFactory) CreateJobsConsumer(consumerId string) (Consumer, error) {
	return CreateRabbitMqConsumer(f.connection, JobsExchange, fmt.Sprintf("jobs_%s", consumerId), "8")
}

func (f *RabbitMqFactory) CreateResultsPublisher() (Publisher, error) {
	return CreateRabbitMqPublisher(f.connection, JobResultsExchange)
}

func (f *RabbitMqFactory) CreateResultsConsumer(consumerId string) (Consumer, error) {
	return CreateRabbitMqConsumer(f.connection, JobResultsExchange, "results", "#")
}

type ZeroMqFactory struct {
	context *ZeroMqContext
}

func (f *ZeroMqFactory) Close() error {
	return f.context.Close()
}

// TODO - get names and ports from viper (jobs -> publisher, results -> reporter)
func (f *ZeroMqFactory) CreateJobsPublisher() (Publisher, error) {
	return CreateZeroMqPublisher(f.context, TcpEndpoint{
		Name:     "*",
		port:     8888,
		isServer: true,
	})
}

func (f *ZeroMqFactory) CreateJobsConsumer(consumerId string) (Consumer, error) {
	return CreateZeroMqConsumer(f.context, TcpEndpoint{
		Name:     "publisher",
		port:     8888,
		isServer: false,
	})
}

func (f *ZeroMqFactory) CreateResultsPublisher() (Publisher, error) {
	return CreateZeroMqPublisher(f.context, TcpEndpoint{
		Name:     "reporter",
		port:     9999,
		isServer: false,
	})
}

func (f *ZeroMqFactory) CreateResultsConsumer(consumerId string) (Consumer, error) {
	return CreateZeroMqConsumer(f.context, TcpEndpoint{
		Name:     "*",
		port:     9999,
		isServer: true,
	})
}
