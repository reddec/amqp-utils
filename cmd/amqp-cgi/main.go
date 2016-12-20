package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var server = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP url to source broker")
var queue = flag.String("queue", "output", "Queue name to consume")
var consumer = flag.String("name", "", "Consumer name")
var retry = flag.Duration("retry", 5*time.Second, "Retry interval for execute again")
var parallel = flag.Int("parallel", 1, "Parallel factors for executing")

func executor(consumer <-chan amqp.Delivery, executable []string, channel *amqp.Channel) {
	for msg := range consumer {
		log.Println("Message", msg.MessageId)
		var err error

		cmd := exec.Command(executable[0], executable[1:]...)

		for header, value := range msg.Headers {
			cmd.Env = append(cmd.Env, fmt.Sprintf("AMQP_%s=%v", header, value))
		}
		cmd.Env = append(cmd.Env, "ContentType="+msg.ContentType)
		cmd.Env = append(cmd.Env, "ContentEncoding="+msg.ContentEncoding)
		cmd.Env = append(cmd.Env, "CorrelationId="+msg.CorrelationId)
		cmd.Env = append(cmd.Env, "ReplyTo="+msg.ReplyTo)
		cmd.Env = append(cmd.Env, "Expiration="+msg.Expiration)
		cmd.Env = append(cmd.Env, "MessageId="+msg.MessageId)
		cmd.Env = append(cmd.Env, "Type="+msg.Type)
		cmd.Env = append(cmd.Env, "Exchange="+msg.Exchange)
		cmd.Env = append(cmd.Env, "RoutingKey="+msg.RoutingKey)

		cmd.Env = append(cmd.Env, fmt.Sprintf("DeliveryMode=%v", msg.DeliveryMode))
		cmd.Env = append(cmd.Env, fmt.Sprintf("Priority=%v", msg.Priority))
		cmd.Env = append(cmd.Env, fmt.Sprintf("Timestamp=%v", msg.Timestamp.Format(time.RFC3339Nano)))

		cmd.Stdin = bytes.NewBuffer(msg.Body)
		out := &bytes.Buffer{}
		if msg.ReplyTo != "" {
			cmd.Stdout = out
		} else {
			cmd.Stdout = os.Stdout
		}
		cmd.Stderr = os.Stderr
		err = cmd.Start()
		if err != nil {
			log.Fatal(err)
		}
		err = cmd.Wait()
		if err != nil {
			log.Println("Something goes run", err)
			err = msg.Nack(false, true) // Re queue
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(*retry)
		} else {
			err = msg.Ack(false)
			if err != nil {
				log.Fatal(err)
			}
		}
		if msg.ReplyTo != "" {
			err := channel.Publish("", msg.ReplyTo, false, false, amqp.Publishing{
				MessageId:     uuid.NewV4().String(),
				Timestamp:     time.Now(),
				CorrelationId: msg.MessageId,
				Body:          out.Bytes(),
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func main() {
	flag.Parse()

	log.SetOutput(os.Stderr)
	if len(flag.Args()) == 0 {
		log.Fatal("You have to specify executable with (optionally) args")
	}

	connection, err := amqp.Dial(*server)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer channel.Close()
	consumer, err := channel.Consume(*queue, *consumer, false, false, true, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	wg := sync.WaitGroup{}

	wg.Add(*parallel)
	for i := 0; i < *parallel; i++ {
		go func() {
			defer wg.Done()
			executor(consumer, flag.Args(), channel)
		}()
	}
	log.Println("Started")
	wg.Wait()
	log.Println("Finished")
}
