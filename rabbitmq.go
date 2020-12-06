package consistenthashing

import (
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type Exchange struct {
	Name string
	Type string
}

var (
	JobsExchange = Exchange{
		Name: "jobs",
		Type: "x-consistent-hash",
	}
	ResultsExchange = Exchange{
		Name: "results",
		Type: "topic",
	}
	TerminateExchange = Exchange{
		Name: "terminate",
		Type: "fanout",
	}
)

type RabbitMqConnection struct {
	connection *amqp.Connection
}

func createRabbitMqConnection(url string) (*RabbitMqConnection, error) {
	var connection *amqp.Connection

	if err := backoff.RetryNotify(
		func() error {
			var err error
			connection, err = amqp.Dial(url)
			return err
		},
		backoff.NewExponentialBackOff(),
		func(err error, duration time.Duration) {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"url":        url,
				"duration":   duration},
			).Error("failed to connect to RabbitMQ, waiting")
		},
	); err != nil {
		return nil, err
	}

	return &RabbitMqConnection{connection: connection}, nil
}

func (c *RabbitMqConnection) Close() error {
	return c.connection.Close()
}

type RabbitMqPublisher struct {
	channel  *amqp.Channel
	exchange Exchange
}

func createRabbitMqPublisher(connection *RabbitMqConnection, exchange Exchange) (*RabbitMqPublisher, error) {
	channel, err := connection.connection.Channel()
	if err != nil {
		return nil, err
	}

	if err := channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	return &RabbitMqPublisher{channel: channel, exchange: exchange}, nil
}

func (p *RabbitMqPublisher) Close() error {
	return p.channel.Close()
}

func (p *RabbitMqPublisher) Publish(routingKey string, message interface{}) error {
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return p.channel.Publish(
		p.exchange.Name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

type RabbitMqConsumer struct {
	channel   *amqp.Channel
	exchange  Exchange
	queueName string
}

func createRabbitMqConsumer(
	connection *RabbitMqConnection,
	exchange Exchange,
	queueName string,
	routingKey string) (*RabbitMqConsumer, error) {

	channel, err := connection.connection.Channel()
	if err != nil {
		return nil, err
	}

	if err := channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	if err := channel.QueueBind(
		queue.Name,
		routingKey,
		exchange.Name,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	return &RabbitMqConsumer{channel: channel, exchange: exchange, queueName: queueName}, nil
}

func (c *RabbitMqConsumer) Close() error {
	return c.channel.Close()
}

func (c *RabbitMqConsumer) Consume(ctx context.Context, incomingMessages chan<- interface{}, newMessagePtr func() interface{}) error {
	deliveries, err := c.channel.Consume(
		c.queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	for {
		select {
		case delivery := <-deliveries:
			messagePtr := newMessagePtr()
			if err := json.Unmarshal(delivery.Body, messagePtr); err != nil {
				return errors.Wrapf(err, "failed to handle message %v", delivery.Body)
			}
			incomingMessages <- messagePtr

		case <-ctx.Done():
			return nil
		}
	}
}
