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
	Publish(messageId string, message interface{}) error
	io.Closer
}

type Consumer interface {
	Consume(ctx context.Context, messageObj interface{}, incomingMessages chan<- interface{}) error
	io.Closer
}

type Factory interface {
	CreateJobsPublisher() (Publisher, error)
	CreateJobsConsumer(consumerId string) (Consumer, error)
	CreateResultsPublisher() (Publisher, error)
	CreateResultsConsumer() (Consumer, error)
	CreateTerminatePublisher() (Publisher, error)
	CreateTerminateConsumer() (Consumer, error)
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
	return createRabbitMqPublisher(f.connection, ResultsExchange)
}

func (f *RabbitMqFactory) CreateResultsConsumer() (Consumer, error) {
	return createRabbitMqConsumer(f.connection, ResultsExchange, "results", "#")
}

func (f *RabbitMqFactory) CreateTerminatePublisher() (Publisher, error) {
	return createRabbitMqPublisher(f.connection, TerminateExchange)
}

func (f *RabbitMqFactory) CreateTerminateConsumer() (Consumer, error) {
	return createRabbitMqConsumer(f.connection, TerminateExchange, "", "")
}

type ZeroMqFactory struct {
	context *ZeroMqContext
}

func (f *ZeroMqFactory) Close() error {
	return f.context.Close()
}

func (f *ZeroMqFactory) CreateJobsPublisher() (Publisher, error) {
	socket, err := createZeroMqRouterSocket(f.context)
	if err != nil {
		return nil, err
	}

	if err = socket.bind(uint16(viper.GetUint32("zeromq_jobs_endpoint_port"))); err != nil {
		return nil, err
	}

	return createConsistentHashingLoadBalancer(socket)
}

func (f *ZeroMqFactory) CreateJobsConsumer(consumerId string) (Consumer, error) {
	socket, err := createZeroMqReqSocket(f.context, consumerId)
	if err != nil {
		return nil, err
	}

	if err = socket.connect(TcpEndpoint{
		Name: viper.GetString("zeromq_jobs_endpoint_name"),
		port: uint16(viper.GetUint32("zeromq_jobs_endpoint_port")),
	}); err != nil {
		return nil, err
	}

	return &ZeroMqConsumer{socket: socket}, nil
}

func (f *ZeroMqFactory) CreateResultsPublisher() (Publisher, error) {
	socket, err := createZeroMqPushSocket(f.context)
	if err != nil {
		return nil, err
	}

	if err = socket.connect(TcpEndpoint{
		Name: viper.GetString("zeromq_results_endpoint_name"),
		port: uint16(viper.GetUint32("zeromq_results_endpoint_port")),
	}); err != nil {
		return nil, err
	}

	return &ZeroMqPublisher{socket: socket}, nil
}

func (f *ZeroMqFactory) CreateResultsConsumer() (Consumer, error) {
	socket, err := createZeroMqPullSocket(f.context)
	if err != nil {
		return nil, err
	}

	if err = socket.bind(uint16(viper.GetUint32("zeromq_results_endpoint_port"))); err != nil {
		return nil, err
	}

	return &ZeroMqConsumer{socket: socket}, nil
}

func (f *ZeroMqFactory) CreateTerminatePublisher() (Publisher, error) {
	socket, err := createZeroMqPubSocket(f.context)
	if err != nil {
		return nil, err
	}

	if err = socket.bind(uint16(viper.GetUint32("zeromq_terminate_endpoint_port"))); err != nil {
		return nil, err
	}

	return &ZeroMqPublisher{socket: socket}, nil
}

func (f *ZeroMqFactory) CreateTerminateConsumer() (Consumer, error) {
	socket, err := createZeroMqSubSocket(f.context, "")
	if err != nil {
		return nil, err
	}

	if err = socket.connect(TcpEndpoint{
		Name: viper.GetString("zeromq_terminate_endpoint_name"),
		port: uint16(viper.GetUint32("zeromq_terminate_endpoint_port")),
	}); err != nil {
		return nil, err
	}

	return &ZeroMqConsumer{socket: socket}, nil
}
