package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
)

type RabbitMQ struct {
	conn *amqp.Connection
	channel *amqp.Channel
	qname string
	exchange string
}

func New(addr string) (*RabbitMQ,error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		log.Printf("Failed to connect rabbitmq server:%s", err)
		return nil, err
	}

	ch,err := conn.Channel()
	if err != nil {
		log.Printf("Failed to create channel by connection:%s", err)
		conn.Close()
		return nil, err
	}

	q,err := ch.QueueDeclare("", false, true, false, false, nil)
	if err != nil {
		log.Printf("Failed to create queue by channel:%s", err)
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &RabbitMQ{conn: conn, channel: ch, qname: q.Name, exchange: ""}, nil
}

func (q *RabbitMQ) Bind(exchange string) error {
	err := q.channel.QueueBind(q.qname, "", exchange, false, nil)
	if err != nil {
		log.Printf("Failed to bind queue to exchange:%s", err)
		return err
	}

	q.exchange = exchange
	return nil
}

func (q *RabbitMQ) Send(queue string, msg interface{}) error {
	body,err := json.Marshal(msg)
	if err != nil {
		log.Printf("Invalid message format:%s", err)
		return err
	}

	err = q.channel.Publish("", queue, false, false, amqp.Publishing{ReplyTo: q.qname,
		Body: body})
	if err != nil {
		log.Printf("Publish message to queue failed:%s", err)
		return err
	}

	return nil
}

func (q *RabbitMQ) Publish(exchange string, msg interface{}) error {
	body,err := json.Marshal(msg)
	if err != nil {
		log.Printf("Invalid message format:%s", err)
		return err
	}

	err = q.channel.Publish(exchange, "", false, false, amqp.Publishing{ReplyTo: q.qname,
		Body: body})
	if err != nil {
		log.Printf("Publish message to exchange failed:%s", err)
		return err
	}

	return nil
}

func (q *RabbitMQ) Consume() (<- chan amqp.Delivery, error) {
	ch,err := q.channel.Consume(q.qname, "", true, false, false, false,
		nil)
	if err != nil {
		log.Printf("Consume message from queue failed:%s", err)
		return nil, err
	}

	return ch,err
}

func (q *RabbitMQ) Close() {
	q.channel.Close()
	q.conn.Close()
}