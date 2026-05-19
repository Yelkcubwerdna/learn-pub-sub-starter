package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshalVal, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Unable to marshal val: %v", err)
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        marshalVal,
	})

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	newChan, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Couldn't open new channel: ", err)
	}

	newQueue, err := newChan.QueueDeclare(
		queueName,
		queueType == Durable,
		queueType == Transient,
		queueType == Transient,
		false,
		nil,
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Couldn't open new queue: ", err)
	}

	err = newChan.QueueBind(queueName,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Couldn't bind queue: ", err)
	}

	return newChan, newQueue, nil
}
