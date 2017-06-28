package main

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/alecthomas/kingpin"
	"fmt"
)

var app = kingpin.New("amqp-push", "Push data to AMQP broker")

// General options
var (
	brokerUrl         = app.Flag("url", "AMQP url to target broker").Short('u').Default("amqp://guest:guest@localhost:5672/").String()
	exchange          = app.Flag("exchange", "Exchange name").Short('e').Default("amq.topic").String()
	exchangeType      = app.Flag("exchange-type", "Exchange type if create").Default("topic").Enum("topic", "direct")
	passive           = app.Flag("passive", "Do not try to create infrastructure").Short('p').Bool()
	once              = app.Flag("once", "Do not reconnect to AMQP").Bool()
	reconnectInterval = app.Flag("interval", "Reconnect interval").Short('w').Default("3s").Duration()
	quiet             = app.Flag("quiet", "Disable verbose logging").Bool()
	key               = app.Arg("routing-key", "Routing key to publish").String()
)

func createInfrastructure(channel *amqp.Channel) error {
	log.Println("Checking exchange", *exchange, "type", *exchangeType)
	err := channel.ExchangeDeclare(*exchange, *exchangeType, true, false, false, false, nil)
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
	name            = app.Flag("app", "Producer name (app name)").Default(defApp()).Short('a').String()
	single          = app.Flag("single", "Use all STDIN as one message").Short('s').Bool()
	headers         = app.Flag("header", "Additional headers").Short('h').StringMap()
	sep             = app.Flag("sep", "Message separator").Default("\n").String()
	zero            = app.Flag("0", "Zero separator").Short('0').Bool()
	correlationID   = app.Flag("correlation-id", "Correlation message ID").String()
	contentType     = app.Flag("content-type", "Content type of message body").String()
	contentEncoding = app.Flag("content-encoding", "Content encoding of body").String()
	replyTo         = app.Flag("reply-to", "Routing key for reply").String()
)

func sendMessage(channel *amqp.Channel, data []byte) error {
	mheaders := map[string]interface{}{}
	for k, v := range *headers {
		mheaders[k] = v
	}
	msg := amqp.Publishing{
		Body:            data,
		Headers:         mheaders,
		Timestamp:       time.Now(),
		MessageId:       uuid.NewV4().String(),
		AppId:           *name,
		CorrelationId:   *correlationID,
		ContentType:     *contentType,
		ContentEncoding: *contentEncoding,
		ReplyTo:         *replyTo,
	}
	log.Println("Sending", msg.MessageId)
	err := channel.Publish(*exchange, *key, false, false, msg)
	if err != nil {
		return err
	}
	println(msg.MessageId)
	log.Println("Sent message", msg.MessageId)
	return nil
}

var lastMessage []byte

func consumeAndSend(channel *amqp.Channel) error {
	if *single {
		log.Println("Waiting for EOF")
		data, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal(err)
		}
		return sendMessage(channel, data)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		if lastMessage == nil {
			log.Println("Reading message")
			data, err := reader.ReadBytes((*sep)[0])
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				return err
			}
			lastMessage = data
		}
		err := sendMessage(channel, lastMessage)
		if err != nil {
			return err
		}
		lastMessage = nil
	}

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
