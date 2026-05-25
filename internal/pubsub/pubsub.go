package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "nackrequeue"
	NackDiscard AckType = "nackdiscard"
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

	table := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	newQueue, err := newChan.QueueDeclare(
		queueName,
		queueType == Durable,
		queueType == Transient,
		queueType == Transient,
		false,
		table,
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {

	cha, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Queue does not exist/isn't bound to the exchange: ", err)
	}

	del, err := cha.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Unable to create deliveries channel: ", err)
	}

	go func() {
		for mes := range del {
			var unmarshalledMessage T
			err = json.Unmarshal(mes.Body, &unmarshalledMessage)
			if err != nil {
				fmt.Printf("Couldn't unmarshal data: ", err)
			}

			ack := handler(unmarshalledMessage)

			switch ack {
			case Ack:
				mes.Ack(false)
				log.Println("Message acknowledged.\n> ")
			case NackRequeue:
				mes.Nack(false, true)
				log.Println("Message not ackknowledged and requeued.\n> ")
			case NackDiscard:
				mes.Nack(false, false)
				log.Println("Message not acknowledged and discarded.\n> ")
			}
		}

	}()

	return nil
}
