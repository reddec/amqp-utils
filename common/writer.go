package common

import (
	"github.com/streadway/amqp"
	"context"
	"time"
	"log"
	"os"
)

type Writer struct {
	Connection        `yaml:",inline"`
	RoutingKey string `yaml:"routing_key"`

	data chan amqp.Publishing
}

func (w *Writer) Init() {
	w.data = make(chan amqp.Publishing)
}

func (writer *Writer) Prepare(channel *amqp.Channel) error {
	err := writer.Exchange.Create(channel)
	if err != nil {
		return err
	}
	return nil
}

func (wr *Writer) Run() error {
	return wr.Open(func(channel *amqp.Channel) error {
		err := wr.Prepare(channel)
		if err != nil {
			return err
		}
		for msg := range wr.data {
			err := channel.Publish(wr.Exchange.Name, wr.RoutingKey, false, false, msg)
			if err != nil {
				return err
			}
		}
		return StopReconnect
	})
}

func (wr *Writer) Write(msg amqp.Publishing) {
	wr.data <- msg
}

func (writer *Writer) Close() error {
	close(writer.data)
	return nil
}
