package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/reddec/amqp-utils/common"
)

var from = flag.String("from", ":9002", "Bind http listener")
var headers = flag.Bool("headers", false, "Print headers before data")
var auths = common.FlagAuths("auth", common.AuthFlags{}, "Authentication pair (repeated) - user:password")

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	writer := csv.NewWriter(os.Stdout)
	if *headers {
		err := writer.Write(common.MessageHeaders())
		if err != nil {
			log.Fatal(err)
		}
		writer.Flush()
	}
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
		req.Body.Close()
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		log.Println("Consumed message")
		columns, err := msg.Columns()
		if err != nil {
			http.Error(resp, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		err = writer.Write(columns)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadGateway)
			log.Fatal("Failed save message", err)
			return
		}
		writer.Flush()
		log.Println("Message saved")
		resp.WriteHeader(http.StatusNoContent)
	})
	log.Println("Ready")
	log.Fatal(http.ListenAndServe(*from, nil))
}
