package consistenthashing

import (
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
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
		Type: "topic",
	}
)

type RabbitMqConnection struct {
	connection *amqp.Connection
}

func CreateRabbitMqConnection(url string) (*RabbitMqConnection, error) {
	var connection *amqp.Connection

	err := backoff.RetryNotify(
		func() error {
			var err error
			connection, err = amqp.Dial(url)
			return err
		}, backoff.NewExponentialBackOff(),
		func(err error, duration time.Duration) {
			log.WithFields(log.Fields{
				log.ErrorKey: err,
				"url":        url,
				"duration":   duration},
			).Error("failed to connect to RabbitMQ, waiting")
		},
	)

	if err != nil {
		return nil, err
	}

	return &RabbitMqConnection{connection: connection}, nil
}

func (c *RabbitMqConnection) Close() {
	if c.connection.IsClosed() {
		return
	}

	c.connection.Close()
}

type RabbitMqPublisher struct {
	channel  *amqp.Channel
	exchange Exchange
}

func CreateRabbitMqPublisher(connection *RabbitMqConnection, exchange Exchange) (*RabbitMqPublisher, error) {
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

func (p *RabbitMqPublisher) Close() {
	p.channel.Close()
}

func (p RabbitMqPublisher) Publish(routingKey string, message interface{}) error {
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

func CreateRabbitMqConsumer(
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

	if _, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	if err := channel.QueueBind(
		queueName,
		routingKey,
		exchange.Name,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	return &RabbitMqConsumer{channel: channel, exchange: exchange, queueName: queueName}, nil
}

func (c *RabbitMqConsumer) Close() {
	c.channel.Close()
}

func (c *RabbitMqConsumer) Consume(ctx context.Context, messagePtr interface{}, onNewMessageCallback func()) error {
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

	handleDeliveries(ctx, messagePtr, onNewMessageCallback, deliveries)
	return nil
}

func handleDeliveries(ctx context.Context, message interface{}, onNewMessageCallback func(), in <-chan amqp.Delivery) {
	for {
		select {
		case delivery := <-in:
			if err := json.Unmarshal(delivery.Body, message); err != nil {
				log.WithFields(log.Fields{
					log.ErrorKey: err,
					"message":    delivery.Body,
				}).Error("failed to handle message")
				continue
			}
			onNewMessageCallback()
		case <-ctx.Done():
			return
		}
	}
}
