package common

import (
	"github.com/streadway/amqp"
	"time"
	"io"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

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
		if err == io.EOF {
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
