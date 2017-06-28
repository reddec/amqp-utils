package main

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/alecthomas/kingpin"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"encoding/json"
)

var app = kingpin.New("amqp-call", "Push data to AMQP broker and wait for response")

// General options
var (
	brokerUrl    = app.Flag("url", "AMQP url to target broker").Short('u').Default("amqp://guest:guest@localhost:5672/").String()
	exchange     = app.Flag("exchange", "Exchange name").Short('e').Default("amq.topic").String()
	exchangeType = app.Flag("exchange-type", "Exchange type if create").Default("topic").Enum("topic", "direct")
	passive      = app.Flag("passive", "Do not try to create infrastructure").Short('p').Bool()
	quiet        = app.Flag("quiet", "Disable verbose logging").Bool()
	key          = app.Arg("routing-key", "Routing key to publish").String()
)

var realQueue = ""

func createInfrastructure(channel *amqp.Channel) error {
	log.Println("Checking exchange", *exchange, "type", *exchangeType)
	err := channel.ExchangeDeclare(*exchange, *exchangeType, true, false, false, false, nil)

	q, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return err
	}
	realQueue = q.Name
	log.Println("Created temprorary queue", realQueue)
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

	return consumeAndSend(channel)
}

// Specific options
var (
	name            = app.Flag("app", "Producer and consumer name (app name)").Default(defApp()).Short('a').String()
	headers         = app.Flag("header", "Additional headers").Short('h').StringMap()
	timeout         = app.Flag("timeout", "Request timeout").Short('t').Default("30s").Duration()
	contentType     = app.Flag("content-type", "Content type of message body").String()
	contentEncoding = app.Flag("content-encoding", "Content encoding of body").String()
	format          = app.Flag("format", "Output format").Default("raw").Enum("json", "raw")
)

func sendMessage(channel *amqp.Channel, data []byte) error {
	mheaders := map[string]interface{}{}
	for k, v := range *headers {
		mheaders[k] = v
	}
	corrId := uuid.NewV4().String()
	msg := amqp.Publishing{
		Body:            data,
		Headers:         mheaders,
		Timestamp:       time.Now(),
		MessageId:       uuid.NewV4().String(),
		AppId:           *name,
		CorrelationId:   corrId,
		ContentType:     *contentType,
		ContentEncoding: *contentEncoding,
		ReplyTo:         realQueue,
	}
	log.Println("Sending", msg.MessageId)
	err := channel.Publish(*exchange, *key, false, false, msg)
	if err != nil {
		return err
	}
	log.Println("Sent message", msg.MessageId)
	return nil
}

func consumeAndSend(channel *amqp.Channel) error {
	log.Println("Waiting for EOF")
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	err = sendMessage(channel, data)
	if err != nil {
		return err
	}
	stream, err := channel.Consume(realQueue, *name, true, true, false, false, nil)
	if err != nil {
		return err
	}
	select {
	case msg, ok := <-stream:
		if !ok {
			return io.EOF
		}
		if msg.Headers != nil {
			if ert, ok := msg.Headers["error"]; ok {
				return errors.New(ert.(string))
			}
		}
		return dumpMessage(msg)
	case <-time.After(*timeout):
		return errors.New("timeout")
	}
}

func dumpMessage(msg amqp.Delivery) error {
	var err error
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
	if *quiet {
		log.SetOutput(ioutil.Discard)
	} else {
		log.SetOutput(os.Stderr)
	}
	var err error
	err = run()
	if err != nil {
		log.Println("Error", err)
	}
	if err != nil {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
