package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"html/template"
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
var timeout = flag.Duration("timeout", 20*time.Second, "HTTP POST request timeout")
var parallel = flag.Int("parallel", 1, "Parallel factors for sending")

var convert = flag.String("template", "", "Template (Go) that prepares message body before send")
var headers = common.FlagMapFlags("header", common.MapFlags{"Content-Type": "application/json"}, "HTTP Header (repeated) in k=v format")

func sender(consumer <-chan amqp.Delivery, retry time.Duration, templ *template.Template) {
	client := &http.Client{Timeout: *timeout}
	for msg := range consumer {
		log.Println("Message", msg.MessageId)
		var data []byte
		var err error

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

		if templ == nil {
			data, err = json.Marshal(toSend)
		} else {
			buf := &bytes.Buffer{}
			err = templ.Execute(buf, toSend)
			data = buf.Bytes()
		}
		if err != nil {
			log.Fatal(err)
		}
		for {
			req, err := http.NewRequest(http.MethodPost, *target, bytes.NewBuffer(data))
			if err != nil {
				log.Fatal("Create request:", err)
			}
			for k, v := range *headers {
				req.Header.Set(k, v)
			}
			response, err := client.Do(req)
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

	var templ *template.Template

	if *convert != "" {
		t, err := template.ParseFiles(*convert)
		if err != nil {
			log.Fatal(err)
		}
		templ = t
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
			sender(consumer, *retry, templ)
		}()
	}
	log.Println("Started")
	wg.Wait()
	log.Println("Finished")
}
