package consistenthashing

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pebbe/zmq4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io"
	"sync/atomic"
	"time"
)

type ZeroMqContext struct {
	context *zmq4.Context
}

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

type TcpEndpoint struct {
	Name string
	port uint16
}

func (e TcpEndpoint) String() string {
	return fmt.Sprintf("tcp://%s:%d", e.Name, e.port)
}

type ZeroMqSocket struct {
	socket *zmq4.Socket
}

func (s *ZeroMqSocket) bind(port uint16) error {
	return s.socket.Bind(TcpEndpoint{Name: "*", port: port}.String())
}

func (s *ZeroMqSocket) connect(endpoint TcpEndpoint) error {
	return s.socket.Connect(endpoint.String())
}

func (s *ZeroMqSocket) Close() error {
	return s.socket.Close()
}

func readMessage(ctx context.Context, socket *zmq4.Socket) ([]byte, error) {
	poller := zmq4.NewPoller()
	poller.Add(socket, zmq4.POLLIN)

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		sockets, err := poller.Poll(1 * time.Second)
		if err != nil {
			return nil, errors.Wrap(err, "failed to poll consumer socket")
		}

		if len(sockets) == 0 {
			continue
		}

		return socket.RecvBytes(zmq4.DONTWAIT)
	}
}

type ZeroMqReqSocket struct {
	*ZeroMqSocket
}

func createZeroMqReqSocket(ctx *ZeroMqContext, identity string) (*ZeroMqReqSocket, error) {
	socket, err := ctx.context.NewSocket(zmq4.REQ)
	if err != nil {
		return nil, err
	}
	if err = socket.SetIdentity(identity); err != nil {
		return nil, err
	}
	return &ZeroMqReqSocket{&ZeroMqSocket{socket: socket}}, nil
}

func (s *ZeroMqReqSocket) readMessage(ctx context.Context) ([]byte, error) {
	if _, err := s.socket.Send("", zmq4.DONTWAIT); err != nil {
		return nil, err
	}
	return readMessage(ctx, s.socket)
}

type ZeroMqPullSocket struct {
	*ZeroMqSocket
}

func createZeroMqPullSocket(ctx *ZeroMqContext) (*ZeroMqPullSocket, error) {
	socket, err := ctx.context.NewSocket(zmq4.PULL)
	if err != nil {
		return nil, err
	}
	return &ZeroMqPullSocket{&ZeroMqSocket{socket: socket}}, nil
}

func (s *ZeroMqPullSocket) readMessage(ctx context.Context) ([]byte, error) {
	return readMessage(ctx, s.socket)
}

type ZeroMqPushSocket struct {
	*ZeroMqSocket
}

func createZeroMqPushSocket(ctx *ZeroMqContext) (*ZeroMqPushSocket, error) {
	socket, err := ctx.context.NewSocket(zmq4.PUSH)
	if err != nil {
		return nil, err
	}
	return &ZeroMqPushSocket{&ZeroMqSocket{socket: socket}}, nil
}

func (s *ZeroMqPushSocket) writeMessage(message []byte) error {
	_, err := s.socket.SendBytes(message, zmq4.DONTWAIT)
	return err
}

type ZeroMqSubSocket struct {
	*ZeroMqSocket
}

func createZeroMqSubSocket(ctx *ZeroMqContext, filter string) (*ZeroMqSubSocket, error) {
	socket, err := ctx.context.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	if err := socket.SetSubscribe(filter); err != nil {
		return nil, err
	}

	return &ZeroMqSubSocket{&ZeroMqSocket{socket: socket}}, nil
}

func (s *ZeroMqSubSocket) readMessage(ctx context.Context) ([]byte, error) {
	return readMessage(ctx, s.socket)
}

type ZeroMqPubSocket struct {
	*ZeroMqSocket
}

func createZeroMqPubSocket(ctx *ZeroMqContext) (*ZeroMqPubSocket, error) {
	socket, err := ctx.context.NewSocket(zmq4.PUB)
	if err != nil {
		return nil, err
	}
	return &ZeroMqPubSocket{&ZeroMqSocket{socket: socket}}, nil
}

func (s *ZeroMqPubSocket) writeMessage(message []byte) error {
	_, err := s.socket.SendBytes(message, zmq4.DONTWAIT)
	return err
}

type ZeroMqRouterSocket struct {
	*ZeroMqSocket
}

func createZeroMqRouterSocket(ctx *ZeroMqContext) (*ZeroMqRouterSocket, error) {
	socket, err := ctx.context.NewSocket(zmq4.ROUTER)
	if err != nil {
		return nil, err
	}
	return &ZeroMqRouterSocket{&ZeroMqSocket{socket: socket}}, nil
}

type consistentHashingLoadBalancer struct {
	consumerQueues *consistentHashingQueues
	socket         *zmq4.Socket
	closingChPtr   atomic.Value
}

func createConsistentHashingLoadBalancer(socket *ZeroMqRouterSocket) (*consistentHashingLoadBalancer, error) {
	res := &consistentHashingLoadBalancer{
		consumerQueues: createConsistentHashingQueues(),
		socket:         socket.socket,
	}

	go res.startLoadBalancer()
	return res, nil
}

func (b *consistentHashingLoadBalancer) Publish(messageId string, message interface{}) error {
	if b.closingChPtr.Load() != nil {
		return fmt.Errorf("cannot publish after closing")
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return backoff.RetryNotify(
		func() error {
			return b.consumerQueues.pushMessage(messageId, data)
		},
		backoff.NewExponentialBackOff(),
		func(err error, duration time.Duration) {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"messageId":  messageId,
				"duration":   duration},
			).Error("failed to push message to queue, waiting")
		},
	)
}

func (b *consistentHashingLoadBalancer) Close() error {
	doneClosing := make(chan interface{})
	b.closingChPtr.Store(doneClosing)
	log.Info("start closing process, now waiting for consumers")
	<-doneClosing
	return b.socket.Close()
}

func (b *consistentHashingLoadBalancer) startLoadBalancer() {
	waitingConsumers := make([]string, 0)

	poller := zmq4.NewPoller()
	poller.Add(b.socket, zmq4.POLLIN)

	for {
		b.registerWaitingConsumer(&waitingConsumers, poller)

		consumersWithPendingMessages := b.consumerQueues.getNodesWithPendingMessages()
		if len(consumersWithPendingMessages) == 0 {
			if consumingCh := b.closingChPtr.Load(); consumingCh != nil {
				consumingCh.(chan interface{}) <- true
				return
			}

			continue
		}

		waitingConsumers = b.sendMessagesToConsumers(waitingConsumers, consumersWithPendingMessages)
	}
}

func (b *consistentHashingLoadBalancer) registerWaitingConsumer(waitingConsumers *[]string, poller *zmq4.Poller) {
	timeout := time.Duration(-1)
	if len(*waitingConsumers) > 0 {
		timeout = 1 * time.Second
	}

	sockets, err := poller.Poll(timeout)
	if err != nil {
		log.WithError(err).Warning("failed to poll consumer socket")
		return
	}

	if len(sockets) == 0 {
		return
	}

	parts, err := b.socket.RecvMessageBytes(0)
	if err != nil {
		log.WithError(err).Warning("failed to receive job request")
		return
	}

	consumerId := string(parts[0])
	b.consumerQueues.ring.addOrVerify(consumerId)
	*waitingConsumers = append(*waitingConsumers, consumerId)
}

func (b *consistentHashingLoadBalancer) sendMessagesToConsumers(waitingConsumers []string, consumersWithPendingMessages map[string]bool) []string {
	newWaitingConsumers := make([]string, 0)
	for _, consumerId := range waitingConsumers {
		if _, ok := consumersWithPendingMessages[consumerId]; !ok {
			newWaitingConsumers = append(newWaitingConsumers, consumerId)
			continue
		}

		message := b.consumerQueues.popNodeMessage(consumerId)

		if _, err := b.socket.SendMessageDontwait(consumerId, "", message); err != nil {
			log.WithError(err).Error("failed to send message")
			newWaitingConsumers = append(newWaitingConsumers, consumerId)
		}
	}
	return newWaitingConsumers
}

type WriterSocket interface {
	writeMessage(message []byte) error
	io.Closer
}

type ZeroMqPublisher struct {
	socket WriterSocket
}

func (p *ZeroMqPublisher) Close() error {
	return p.socket.Close()
}

func (p *ZeroMqPublisher) Publish(_ string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return p.socket.writeMessage(data)
}

type ReaderSocket interface {
	readMessage(ctx context.Context) ([]byte, error)
	io.Closer
}

type ZeroMqConsumer struct {
	socket ReaderSocket
}

func (c *ZeroMqConsumer) Close() error {
	return c.socket.Close()
}

func (c *ZeroMqConsumer) Consume(ctx context.Context, incomingMessages chan<- interface{}, newMessagePtr func() interface{}) error {
	for {
		data, err := c.socket.readMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errors.Wrap(err, "failed to read message")
		}

		messagePtr := newMessagePtr()
		if err := json.Unmarshal(data, messagePtr); err != nil {
			return errors.Wrapf(err, "failed to handle message %v", data)
		}

		incomingMessages <- messagePtr
	}
}
