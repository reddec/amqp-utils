package main

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
	"github.com/alecthomas/kingpin"
	"fmt"
	"encoding/json"
	"io"
)

var app = kingpin.New("amqp-cat", "Read data from AMQP broker")

// General options
var (
	brokerUrl         = app.Flag("url", "AMQP url to target broker").Short('u').Default("amqp://guest:guest@localhost:5672/").String()
	exchange          = app.Flag("exchange", "Exchange name").Short('e').Default("amq.topic").String()
	exchangeType      = app.Flag("exchange-type", "Exchange type if create").Default("topic").Enum("topic", "direct")
	passive           = app.Flag("passive", "Do not try to create infrastructure").Short('p').Bool()
	queue             = app.Flag("queue", "Queue name (empty for auto generate). Always enables --rm").Short('q').String()
	removeQueue       = app.Flag("rm", "Remove queue auto disconnect (non-durable)").Bool()
	exclusive         = app.Flag("exclusive", "Only one consumer can use queue").Bool()
	once              = app.Flag("once", "Do not reconnect to AMQP").Bool()
	lazy              = app.Flag("lazy", "Lazy queue (RabbitMQ only)").Short('l').Bool()
	reconnectInterval = app.Flag("interval", "Reconnect interval").Short('w').Default("3s").Duration()
	quiet             = app.Flag("quiet", "Disable verbose logging").Short('v').Bool()
	keys              = app.Arg("routing-key", "Routing keys to bind").Strings()
)

var realQueue = ""

func createInfrastructure(channel *amqp.Channel) error {
	log.Println("Checking exchange", *exchange, "type", *exchangeType)
	err := channel.ExchangeDeclare(*exchange, *exchangeType, true, false, false, false, nil)
	autoDelete := *removeQueue || *queue == ""
	log.Println("Checking queue (auto-delete -", autoDelete, ")")
	args := make(amqp.Table)
	if *lazy {
		args["x-queue-mode"] = "lazy"
	}
	q, err := channel.QueueDeclare(*queue, true, autoDelete, *exclusive, false, args)
	if err != nil {
		return err
	}
	realQueue = q.Name
	log.Println("Queue is", realQueue)
	for _, key := range *keys {
		log.Println("Bindingig queue with key", key)
		err = channel.QueueBind(realQueue, key, *exchange, false, nil)
		if err != nil {
			return err
		}
	}
	return err
}

func run() error {
	log.Println("Connecting")
	conn, err := amqp.Dial(*brokerUrl)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Println("Opening channel")
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if !*passive {
		log.Println("Creating infrastructure if required")
		err = createInfrastructure(channel)
		if err != nil {
			return err
		}
	} else {
		log.Println("Skip check infrastructure")
	}
	return receiveMessages(channel)
}

// Specific options
var (
	name   = app.Flag("app", "Consumer name (app name)").Default(defApp()).Short('a').String()
	single = app.Flag("single", "Consume only one message").Short('1').Bool()
	sep    = app.Flag("sep", "Message separator").Default("\n").String()
	zero   = app.Flag("0", "Zero separator").Short('0').Bool()
	format = app.Flag("format", "Output format").Default("raw").Enum("json", "raw")
)

func receiveMessages(channel *amqp.Channel) error {
	log.Println("Start receiveing messages")
	stream, err := channel.Consume(realQueue, *name, false, *exclusive, false, false, nil)
	if err != nil {
		return err
	}
	for msg := range stream {
		err = msg.Ack(false)
		if err != nil {
			return err
		}
		err = dumpMessage(msg)
		if err != nil {
			return err
		}
		if *single {
			return nil
		}
	}
	if err == nil {
		err = io.EOF
	}
	return err
}

var firstMessage = true

func dumpMessage(msg amqp.Delivery) error {
	var err error
	if !firstMessage {
		print(*sep)
	} else {
		firstMessage = false
	}
	switch *format {
	case "raw":
		_, err = os.Stdout.Write(msg.Body)
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "    ")
		err = enc.Encode(msg)
	default:
		panic("Unknown message format")
	}
	return err
}

func defApp() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%v@%v", host, os.Getpid())
}

func main() {
	app.DefaultEnvars()
	kingpin.MustParse(app.Parse(os.Args[1:]))
	if *zero {
		*sep = "\000"
	}
	if *quiet {
		log.SetOutput(ioutil.Discard)
	} else {
		log.SetOutput(os.Stderr)
	}
	var err error
	for {
		err = run()
		if *once || err == nil {
			break
		}
		log.Println("Error", err, "- waiting", *reconnectInterval)
		time.Sleep(*reconnectInterval)
	}
	if err != nil {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
