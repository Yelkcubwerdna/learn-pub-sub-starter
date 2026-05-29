package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        marshalVal,
	})
	if err != nil {
		return err
	}

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

func decodeJSON[T any](msg []byte) (T, error) {
	var unmarshalledMessage T

	err := json.Unmarshal(msg, &unmarshalledMessage)
	if err != nil {
		return unmarshalledMessage, err
	}

	return unmarshalledMessage, nil
}

func decodeGob[T any](msg []byte) (T, error) {
	var buffer bytes.Buffer
	var result T

	_, err := buffer.Write(msg)
	if err != nil {
		return result, err
	}

	dec := gob.NewDecoder(&buffer)
	err = dec.Decode(&result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		decodeJSON,
	)

	return err
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		decodeGob,
	)

	return err
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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
			result, err := unmarshaller(mes.Body)
			if err != nil {
				fmt.Println("Couldn't unmarshal message: ", err)
			}

			ack := handler(result)

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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer

	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	encodeVal := buffer.Bytes()

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        encodeVal,
	})
	if err != nil {
		return err
	}

	return nil
}
