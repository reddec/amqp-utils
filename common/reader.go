package common

import (
	"log"
	"github.com/streadway/amqp"
)

type Queue struct {
	Name          string `yaml:"name"`
	Durable       bool   `yaml:"durable"`
	AutoRemove    bool   `yaml:"autoremove"`
	Exclusive     bool   `yaml:"exclusive"`
	Lazy          bool   `yaml:"lazy"`
	Passive       bool   `yaml:"passive"`
	generatedName string
	Binding       map[string][]string `yaml:"binding"`
}

func (q *Queue) RealName() string {
	if q.generatedName == "" {
		return q.Name
	}
	return q.generatedName
}

func (q *Queue) Create(channel *amqp.Channel) error {
	if !q.Passive {
		log.Println("Creating queue")
		if q.Name == "" {
			q.Durable = false
			q.AutoRemove = true
			q.Exclusive = true
		}
		args := make(amqp.Table)
		if q.Lazy {
			args["x-queue-mode"] = "lazy"
		}
		qq, err := channel.QueueDeclare(q.Name, q.Durable, q.AutoRemove, q.Exclusive, false, args)
		if err != nil {
			return err
		}
		q.generatedName = qq.Name
		log.Println("Queue name", qq.Name)
	}
	for exchange, keys := range q.Binding {
		for _, key := range keys {
			log.Println("Binding queue to exchange", exchange, "with routing key", key)
			err := channel.QueueBind(q.RealName(), key, exchange, false, nil)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Reader struct {
	Connection  `yaml:",inline"`
	Queue Queue `yaml:"queue"`
}

func (reader *Reader) Prepare(channel *amqp.Channel) error {
	err := reader.Exchange.Create(channel)
	if err != nil {
		return err
	}
	err = reader.Queue.Create(channel)
	if err != nil {
		return err
	}
	return nil
}

func (r *Reader) Consume(autoAck bool, handler func(<-chan amqp.Delivery) error) error {
	return r.Open(func(channel *amqp.Channel) error {
		err := r.Prepare(channel)
		if err != nil {
			return err
		}
		stream, err := channel.Consume(r.Queue.RealName(), "", autoAck, false, false, false, nil)
		if err != nil {
			return err
		}
		log.Println("Ready to consume")
		return handler(stream)
	})
}
