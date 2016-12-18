package main

import (
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/reddec/amqp-utils/common"
	"github.com/streadway/amqp"
)

var server = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "AMQP url to target broker")
var key = flag.String("key", "", "Routing key to publish. If not set - queue from message will be set")
var exchange = flag.String("exchange", "", "Exchange name. If not set - exchange from message will be set")
var from = flag.String("from", ":9002", "Bind http listener")
var name = flag.String("name", "", "Producer name (app name)")
var auths = common.FlagAuths("auth", common.AuthFlags{}, "Authentication pair (repeated) - user:password")

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
	if len(*auths) > 0 {
		log.Println("HTTP Basic Auth activated")
	} else {
		log.Println("No Auth activated")
	}
	http.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		if !auths.CheckHTTP(req) {
			io.Copy(ioutil.Discard, req.Body)
			http.Error(resp, "", http.StatusForbidden)
			return

		}

		var msg common.Message
		decoder := json.NewDecoder(req.Body)
		err := decoder.Decode(&msg)
		io.Copy(ioutil.Discard, req.Body)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		log.Println("Consumed message")
		targetKey := *key
		if targetKey == "" {
			targetKey = msg.RoutingKey
		}
		targetExchange := *exchange
		if targetExchange == "" {
			targetExchange = msg.Exchange
		}
		// Restore message
		pub := amqp.Publishing{}

		pub.Body = []byte(msg.Body)
		pub.Headers = msg.Headers
		pub.ContentType = msg.ContentType
		pub.ContentEncoding = msg.ContentEncoding
		pub.DeliveryMode = msg.DeliveryMode
		pub.Priority = msg.Priority
		pub.CorrelationId = msg.CorrelationId
		pub.ReplyTo = msg.ReplyTo
		pub.Expiration = msg.Expiration
		pub.MessageId = msg.MessageId
		pub.Timestamp = msg.Timestamp
		pub.Type = msg.Type
		// Extra field
		pub.AppId = *name

		err = channel.Publish(targetExchange, targetKey, false, false, pub)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadGateway)
			log.Fatal(err)
			return
		}
		log.Println("Message pushed", targetExchange, "::", targetKey)
		resp.WriteHeader(http.StatusNoContent)
	})
	log.Println("Ready")
	log.Fatal(http.ListenAndServe(*from, nil))
}
