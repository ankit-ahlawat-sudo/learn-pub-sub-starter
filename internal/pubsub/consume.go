package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, 
	queueName, 
	key string, 
	queueType SimpleQueueType, 
	handler func(T),
) error {
	ch, queue, err:= DeclareAndBind(conn, exchange, queueName, key, queueType)

	deliveryCh, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Not able to create delivery Channel: %+v", err)
	}

	go func () {
		defer ch.Close()
		for delivery := range deliveryCh {
			var dat T
			err:= json.Unmarshal(delivery.Body, &dat) 
			if err != nil {
				fmt.Printf("not able to unmarshal JSON: %v", err)
				continue
			}
			handler(dat)
			delivery.Ack(false)
		}
	} ()
	
	return nil
}


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