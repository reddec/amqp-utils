package common

import (
	"github.com/streadway/amqp"
	"time"
	"log"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/pkg/errors"
)

var StopReconnect = errors.New("Reconnect stopped by application")

type Exchange struct {
	Name       string `yaml:"name"`
	Type       string `yaml:"type"`
	Durable    bool   `yaml:"durable"`
	AutoDelete bool   `yaml:"autodelete"`
	Passive    bool   `yaml:"passive"`
}

func (e *Exchange) Create(channel *amqp.Channel) error {
	if e.Passive {
		return nil
	}
	if e.Type == "" {
		e.Type = "topic"
	}
	log.Println("Creating exchange", e)
	return channel.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, false, false, nil)
}

func (e *Exchange) String() string {
	return e.Name + "@" + e.Type
}

type Reconnect struct {
	Interval time.Duration  `yaml:"interval"`
	Disable  bool           `yaml:"disable"`
}

func (r *Reconnect) Wait() {
	if r.Interval == 0 {
		time.Sleep(10 * time.Second)
	} else {
		time.Sleep(r.Interval)
	}
}

type ChannelHandlerFunc func(channel *amqp.Channel) (error)

type Connection struct {
	URL       string    `yaml:"url"`
	Reconnect Reconnect `yaml:"reconnect"`
	Exchange  Exchange  `yaml:"exchange"`
}

func (c *Connection) connectionOpened(conn *amqp.Connection, handler ChannelHandlerFunc) error {
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	log.Println("Channel opened")
	return handler(ch)

}

func (c *Connection) Open(handler ChannelHandlerFunc) error {
	for {
		log.Println("Connecting to", c.URL)
		conn, err := amqp.Dial(c.URL)
		if err == nil {
			log.Println("Connected")
			err = c.connectionOpened(conn, handler)
		}
		if err == StopReconnect {
			return nil
		}
		if !c.Reconnect.Disable {
			log.Println("Stopped", err, ". Waiting...")
			c.Reconnect.Wait()
		} else {
			return err
		}
	}
}

func MustRead(configYamlFile string, v interface{}) {
	data, err := ioutil.ReadFile(configYamlFile)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(data, v)
	if err != nil {
		panic(err)
	}
}
