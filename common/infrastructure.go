package common

import (
	"github.com/streadway/amqp"
	"context"
)

type Exchange struct {
	Name       string `yaml:"name"`
	Durable    bool   `yaml:"durable"`
	Mode       string `yaml:"mode"`
	AutoDelete bool   `yaml:"auto_delete"`
}

func (exc *Exchange) Serve(channel *amqp.Channel, ctx context.Context) error {
	return channel.ExchangeDeclare(exc.Name, exc.Mode, exc.Durable, exc.AutoDelete, false, false, nil)
}

type Queue struct {
	Name       string `yaml:"name"`
	Bind       []Bind `yaml:"bind"`
	Lazy       bool   `yaml:"lazy"`
	Durable    bool   `yaml:"durable"`
	AutoDelete bool   `yaml:"auto_delete"`
	Exclusive  bool   `yaml:"exclusive"`
}

func (q *Queue) Serve(channel *amqp.Channel, ctx context.Context) error {
	var args amqp.Table
	if q.Lazy {
		args = amqp.Table{
			"x-queue-mode": "lazy",
		}
	}
	_, err := channel.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, false, args)
	return err
}

type Bind struct {
	Queue      string `yaml:"queue"`
	Exchange   string `yaml:"exchange"`
	RoutingKey string `yaml:"key"`
}

func (bnd *Bind) Serve(channel *amqp.Channel, ctx context.Context) error {
	return channel.QueueBind(bnd.Queue, bnd.RoutingKey, bnd.Exchange, false, nil)
}

type Infrastructure struct {
	Exchanges []Exchange `yaml:"exchanges"`
	Queues    []Queue    `yaml:"queues"`
	Binds     []Bind     `yaml:"binds"`
}

func (inf *Infrastructure) Serve(channel *amqp.Channel, ctx context.Context) error {
	for _, ex := range inf.Exchanges {
		if err := ex.Serve(channel, ctx); err != nil {
			return err
		}
	}
	for _, q := range inf.Queues {
		if err := q.Serve(channel, ctx); err != nil {
			return err
		}
	}
	for _, b := range inf.Binds {
		if err := b.Serve(channel, ctx); err != nil {
			return err
		}
	}
	return nil
}
