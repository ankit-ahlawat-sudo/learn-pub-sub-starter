package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshaledJson, err := json.Marshal(val);
	if err != nil {
		return err
	}

	 return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		Body: marshaledJson,
		ContentType: "application/json",
	})
}

type SimpleQueueType string

const (
	Durable SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, _ := conn.Channel();
	var isDurable bool;
	if queueType == Durable {
		isDurable = true
	}
	queue, err:= ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, nil)
	if err != nil {
		return nil, queue, err
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, queue, err
	}
	return  ch, queue, nil
}