package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"io/ioutil"
	"io"
	"github.com/alecthomas/kingpin"
	"strings"
)

var app = kingpin.New("amqp-cgi", "Read data from AMQP broker and execute script")

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
	keys              = app.Flag("routing-key", "Routing keys to bind").Short('k').Strings()
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
	name          = app.Flag("app", "Consumer name (app name)").Default(defApp()).Short('a').String()
	single        = app.Flag("single", "Consume only one message").Short('1').Bool()
	errorStrategy = app.Flag("fail", "Action if non-zero exit code").Short('f').Default("reply").Enum("drop", "restart", "stop", "reply")
	command       = app.Arg("command", "Command that will be run on input").Required().Strings()
)

func receiveMessages(channel *amqp.Channel) error {
	log.Println("Start receiveing messages")
	stream, err := channel.Consume(realQueue, *name, false, *exclusive, false, false, nil)
	if err != nil {
		return err
	}
	for msg := range stream {
		log.Println("Got", msg.MessageId, "from", msg.AppId)
		err = executeScript(msg, channel)

		if err != nil {
			if _, ok := err.(*exec.ExitError); ok {
				switch *errorStrategy {
				case "drop":
					// continue
					log.Println("Drop failed command")
					err = nil
				case "stop":
					log.Println("Stop after failed command")
					*once = false
					return err
				case "restart":
					log.Println("Restart after failed command")
					return err
				case "reply":
					log.Println("Failed result reply to sender (if possible)")
					if msg.ReplyTo != "" {
						err = channel.Publish("", msg.ReplyTo, false, false, amqp.Publishing{
							MessageId:     uuid.NewV4().String(),
							Timestamp:     time.Now(),
							CorrelationId: msg.CorrelationId,
							Headers:       amqp.Table{"error": err.Error()},
							Body:          []byte(err.Error()),
						})
					} else {
						err = nil
					}
				default:
					panic("Unknown strategy " + *errorStrategy)
				}
			} else {
				return err
			}
		}
		if err != nil {
			break
		}
		err = msg.Ack(false)
		if *single {
			return err
		}

	}
	if err == nil {
		err = io.EOF
	}
	return err
}

func executeScript(msg amqp.Delivery, channel *amqp.Channel) error {
	cmd := exec.Command((*command)[0], (*command)[1:]...)
	log.Println(*command)
	for header, value := range msg.Headers {
		cmd.Env = append(cmd.Env, fmt.Sprintf("AMQP_%s=%v", strings.ToUpper(header), value))
	}
	cmd.Env = append(cmd.Env, "CONTENT_TYPE="+msg.ContentType)
	cmd.Env = append(cmd.Env, "CONTENT_ENCODING="+msg.ContentEncoding)
	cmd.Env = append(cmd.Env, "CORRELATION_ID="+msg.CorrelationId)
	cmd.Env = append(cmd.Env, "REPLY_TO="+msg.ReplyTo)
	cmd.Env = append(cmd.Env, "EXPIRATION="+msg.Expiration)
	cmd.Env = append(cmd.Env, "MESSAGE_ID="+msg.MessageId)
	cmd.Env = append(cmd.Env, "TYPE="+msg.Type)
	cmd.Env = append(cmd.Env, "EXCHANGE="+msg.Exchange)
	cmd.Env = append(cmd.Env, "ROUTING_KEY="+msg.RoutingKey)

	cmd.Env = append(cmd.Env, fmt.Sprintf("DELIVERY_MODE=%v", msg.DeliveryMode))
	cmd.Env = append(cmd.Env, fmt.Sprintf("PRIORITY=%v", msg.Priority))
	cmd.Env = append(cmd.Env, fmt.Sprintf("TIMESTAMP=%v", msg.Timestamp.Format(time.RFC3339Nano)))

	cmd.Stdin = bytes.NewBuffer(msg.Body)
	out := &bytes.Buffer{}
	if msg.ReplyTo != "" {
		cmd.Stdout = out
	}
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Println("Execution failed")
		return err
	}
	if msg.ReplyTo != "" {
		return channel.Publish("", msg.ReplyTo, false, false, amqp.Publishing{
			MessageId:     uuid.NewV4().String(),
			Timestamp:     time.Now(),
			CorrelationId: msg.CorrelationId,
			Body:          out.Bytes(),
		})
	}
	return nil
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
