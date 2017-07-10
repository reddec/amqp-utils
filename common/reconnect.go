package common

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"context"
	"time"
)

type ChannelHandlerF func(channel *amqp.Channel, ctx context.Context) error

type ChannelHandler interface {
	Serve(channel *amqp.Channel, ctx context.Context) error
}

type AConn struct {
	URL              string
	ReconnectTimeout time.Duration
	handlers         []ChannelHandlerF
}

func (ac *AConn) AddHandlerFunc(handler ChannelHandlerF) {
	ac.handlers = append(ac.handlers, handler)
}

func (ac *AConn) AddHandler(handler ChannelHandler) {
	ac.handlers = append(ac.handlers, handler.Serve)
}

func (ac *AConn) Serve(ctx context.Context) {
	logger := log.New(os.Stderr, "["+ac.URL+"] ", log.LstdFlags)
LOOP:
	for {
		err := ac.openConnection(ctx)
		if err == nil {
			logger.Println("Connection closed due to", err)
		} else {
			logger.Println("Connection closed due to no active tasks")
		}
		log.Println("Waiting", ac.ReconnectTimeout, "before reconnect")
		select {
		case <-time.After(ac.ReconnectTimeout):
		case <-ctx.Done():
			break LOOP
		}
	}
}

func (ac *AConn) openConnection(ctx context.Context) error {

	conn, err := amqp.Dial(ac.URL)
	if err != nil {
		return err
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	done := make(chan struct{}, 1)
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			channel.Close()
			conn.Close()
		}
	}()

	for _, handler := range ac.handlers {
		err = handler(channel, ctx)
		if err != nil {
			return err
		}
	}
	close(done)
	return nil
}
