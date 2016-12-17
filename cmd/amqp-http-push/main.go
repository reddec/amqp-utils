package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/reddec/amqp-utils/common"
	"github.com/streadway/amqp"
)

var server = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP url to source broker")
var queue = flag.String("queue", "output", "Queue name to consume")
var target = flag.String("to", "http://127.0.0.1:9002/a/b/c", "Target URL to push message")
var consumer = flag.String("name", "", "Consumer name")
var retry = flag.Duration("retry", 5*time.Second, "Retry interval for push to HTTP server")
var parallel = flag.Int("parallel", 1, "Parallel factors for sending")

func sender(consumer <-chan amqp.Delivery, retry time.Duration) {
	for msg := range consumer {
		log.Println("Message", msg.MessageId)
		toSend := common.Message{}
		toSend.Body = string(msg.Body)
		toSend.Headers = msg.Headers
		toSend.ContentType = msg.ContentType
		toSend.ContentEncoding = msg.ContentEncoding
		toSend.DeliveryMode = msg.DeliveryMode
		toSend.Priority = msg.Priority
		toSend.CorrelationId = msg.CorrelationId
		toSend.ReplyTo = msg.ReplyTo
		toSend.Expiration = msg.Expiration
		toSend.MessageId = msg.MessageId
		toSend.Timestamp = msg.Timestamp
		toSend.Type = msg.Type
		toSend.Exchange = msg.Exchange
		toSend.RoutingKey = msg.RoutingKey

		data, err := json.Marshal(toSend)
		if err != nil {
			log.Fatal(err)
		}
		for {
			response, err := http.Post(*target, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Println(err)
				time.Sleep(retry)
				continue
			}
			io.Copy(os.Stdout, response.Body)
			response.Body.Close()
			if response.StatusCode/100 != 2 {
				log.Println("Unsuccess status code:", response.StatusCode, response.Status)
				time.Sleep(retry)
				continue
			}
			log.Println("Sent")
			break
		}
		err = msg.Ack(false)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
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
			sender(consumer, *retry)
		}()
	}
	log.Println("Started")
	wg.Wait()
	log.Println("Finished")
}
