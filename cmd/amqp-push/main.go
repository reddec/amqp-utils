package main

import (
	"bufio"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/reddec/amqp-utils/common"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var server = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP url to target broker")
var key = flag.String("key", "", "Routing key to publish")
var exchange = flag.String("exchange", "", "Exchange name")
var name = flag.String("name", "", "Producer name (app name)")
var single = flag.Bool("single", false, "Use all STDIN as one message")
var headers = common.FlagMapFlags("header", common.MapFlags{}, "Additional header to message (repeated)")

func sendMessage(channel *amqp.Channel, data []byte) {
	mheaders := map[string]interface{}{}
	for k, v := range *headers {
		mheaders[k] = v
	}
	msg := amqp.Publishing{
		Body:      data,
		Headers:   mheaders,
		Timestamp: time.Now(),
		MessageId: uuid.NewV4().String(),
		AppId:     *name,
	}
	err := channel.Publish(*exchange, *key, false, false, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Sent message", msg.MessageId)
}

func main() {
	flag.Parse()
	if *exchange == "" && *key == "" {
		log.Fatal("Required at least exchnage or key")
	}
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
	if *single {
		data, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal(err)
		}
		sendMessage(channel, data)
	} else {
		reader := bufio.NewReader(os.Stdin)
		for {
			data, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			sendMessage(channel, data)
		}
	}
}
