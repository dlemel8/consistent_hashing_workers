package consistenthashing

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pebbe/zmq4"
	log "github.com/sirupsen/logrus"
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

type ZeroMqPublisher struct {
	socket *zmq4.Socket
}

func createZeroMqPublisher(context *ZeroMqContext, endpoint TcpEndpoint) (*ZeroMqPublisher, error) {
	socket, err := createSocket(context, endpoint, zmq4.PUSH)
	if err != nil {
		return nil, err
	}
	return &ZeroMqPublisher{socket}, nil
}

func (p *ZeroMqPublisher) Close() error {
	return p.socket.Close()
}

func (p *ZeroMqPublisher) Publish(messageId string, message interface{}) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = p.socket.SendBytes(data, 0)
	return err
}

type ZeroMqConsumer struct {
	socket *zmq4.Socket
}

func createZeroMqConsumer(ctx *ZeroMqContext, endpoint TcpEndpoint) (*ZeroMqConsumer, error) {
	socket, err := createSocket(ctx, endpoint, zmq4.PULL)
	if err != nil {
		return nil, err
	}
	return &ZeroMqConsumer{socket}, nil
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

		data, err := c.socket.RecvBytes(0)
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
