package consistenthashing

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pebbe/zmq4"
	log "github.com/sirupsen/logrus"
	"time"
)

type TcpEndpoint struct {
	Name     string
	port     uint16
	isServer bool
}

func (e *TcpEndpoint) String() string {
	return fmt.Sprintf("tcp://%s:%d", e.Name, e.port)
}

type ZeroMqContext struct {
	context *zmq4.Context
}

type publishConsumeAlgorithm interface {
	publisherType() zmq4.Type
	consumerType() zmq4.Type
	publish(socket *zmq4.Socket, messageId string, message []byte) error
	consume(socket *zmq4.Socket) ([]byte, error)
}

type roundRobinAlgorithm struct{}

func (a *roundRobinAlgorithm) publisherType() zmq4.Type {
	return zmq4.PUSH
}

func (a *roundRobinAlgorithm) consumerType() zmq4.Type {
	return zmq4.PULL
}

func (a *roundRobinAlgorithm) publish(socket *zmq4.Socket, _ string, message []byte) error {
	_, err := socket.SendBytes(message, 0)
	return err
}

func (a *roundRobinAlgorithm) consume(socket *zmq4.Socket) ([]byte, error) {
	return socket.RecvBytes(0)
}

type consistentHashingAlgorithm struct {
	consumerIdQueues *consistentHashingQueues
}

func (a *consistentHashingAlgorithm) publisherType() zmq4.Type {
	return zmq4.ROUTER
}

func (a *consistentHashingAlgorithm) consumerType() zmq4.Type {
	return zmq4.REQ
}

func (a *consistentHashingAlgorithm) publish(_ *zmq4.Socket, messageId string, message []byte) error {
	var queue chan []byte

	if err := backoff.RetryNotify(
		func() error {
			var err error
			queue, err = a.consumerIdQueues.getQueueByKey(messageId)
			return err
		},
		backoff.NewExponentialBackOff(),
		func(err error, duration time.Duration) {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"messageId":  messageId,
				"duration":   duration},
			).Error("failed to get queue, waiting")
		},
	); err != nil {
		return err
	}

	queue <- message

	return nil
}

func (a *consistentHashingAlgorithm) consume(socket *zmq4.Socket) ([]byte, error) {
	if _, err := socket.Send("job request", 0); err != nil {
		return nil, err
	}
	return socket.RecvBytes(0)
}

func (a *consistentHashingAlgorithm) startProxy(socket *zmq4.Socket) error {
	waitingConsumers := make([]string, 0)

	poller := zmq4.NewPoller()
	poller.Add(socket, zmq4.POLLIN)

	for {
		timeout := time.Duration(-1)
		if len(waitingConsumers) > 0 {
			timeout = time.Duration(1) * time.Second
		}

		sockets, err := poller.Poll(timeout)
		if err != nil {
			log.WithError(err).Warning("failed to poll consumer socket, retry")
			continue
		}

		if len(sockets) > 0 {
			parts, err := socket.RecvMessageBytes(0)
			if err != nil {
				log.WithError(err).Warning("failed to receive job request")
				continue
			}

			consumerId := string(parts[0])
			a.consumerIdQueues.ring.addOrVerify(consumerId)
			waitingConsumers = append(waitingConsumers, consumerId)
		}

		if len(waitingConsumers) == 0 {
			continue
		}

		consumersWithPendingMessages := a.consumerIdQueues.getNodesWithPendingMessages()
		if len(consumersWithPendingMessages) == 0 {
			continue
		}

		newWaitingConsumers := make([]string, 0)
		for _, consumerId := range waitingConsumers {
			if _, ok := consumersWithPendingMessages[consumerId]; !ok {
				newWaitingConsumers = append(newWaitingConsumers, consumerId)
				continue
			}

			message := a.consumerIdQueues.popNodeMessage(consumerId)

			if _, err := socket.SendMessage(consumerId, "", message); err != nil {
				log.WithError(err).Error("failed to send requested job")
			}
		}
		waitingConsumers = newWaitingConsumers
	}
}

var (
	jobsAlgorithm    = &consistentHashingAlgorithm{createConsistentHashingQueues()}
	resultsAlgorithm = &roundRobinAlgorithm{}
)

func createZeroMqContext() (*ZeroMqContext, error) {
	ctx, err := zmq4.NewContext()
	if err != nil {
		return nil, err
	}
	return &ZeroMqContext{ctx}, nil
}

func (c *ZeroMqContext) Close() error {
	return c.context.Term()
}

// TODO - refactor this api to create socket(with optional identity) with bind, connect, send, receive
type ZeroMqPublisher struct {
	socket      *zmq4.Socket
	publishFunc func(socket *zmq4.Socket, messageId string, message []byte) error
}

func createZeroMqPublisher(context *ZeroMqContext, endpoint TcpEndpoint, algorithm publishConsumeAlgorithm) (*ZeroMqPublisher, error) {
	socket, err := createSocket(context, endpoint, algorithm.publisherType())
	if err != nil {
		return nil, err
	}
	return &ZeroMqPublisher{socket: socket, publishFunc: algorithm.publish}, nil
}

func (p *ZeroMqPublisher) Close() error {
	// TODO - we currently use proxy inside job publisher so we need to wait forever - fix!
	x := make(chan bool)
	<-x
	return p.socket.Close()
}

func (p *ZeroMqPublisher) Publish(messageId string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return p.publishFunc(p.socket, messageId, data)
}

type ZeroMqConsumer struct {
	socket      *zmq4.Socket
	consumeFunc func(socket *zmq4.Socket) ([]byte, error)
}

func createZeroMqConsumer(ctx *ZeroMqContext, endpoint TcpEndpoint, algorithm publishConsumeAlgorithm) (*ZeroMqConsumer, error) {
	socket, err := createSocket(ctx, endpoint, algorithm.consumerType())
	if err != nil {
		return nil, err
	}
	return &ZeroMqConsumer{socket: socket, consumeFunc: algorithm.consume}, nil
}

func (c *ZeroMqConsumer) Close() error {
	return c.socket.Close()
}

func (c *ZeroMqConsumer) Consume(ctx context.Context, messagePtr interface{}, onNewMessageCallback func()) error {
	for {
		if ctx.Err() != nil {
			log.Info("consuming has canceled")
			return nil
		}

		data, err := c.consumeFunc(c.socket)
		if err != nil {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"message":    data,
			}).Error("failed to receive message")
			continue
		}

		if err := json.Unmarshal(data, messagePtr); err != nil {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"message":    data,
			}).Error("failed to handle message")
			continue
		}

		onNewMessageCallback()
	}
}

func createSocket(ctx *ZeroMqContext, endpoint TcpEndpoint, type_ zmq4.Type) (*zmq4.Socket, error) {
	socket, err := ctx.context.NewSocket(type_)
	if err != nil {
		return nil, err
	}

	if endpoint.isServer {
		err = socket.Bind(endpoint.String())
	} else {
		err = socket.Connect(endpoint.String())
	}
	return socket, err
}
