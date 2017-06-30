package main

import (
	"../../common"
	"github.com/alecthomas/kingpin"
	"os"
	"log"
	"io/ioutil"
	"github.com/streadway/amqp"
	"net/smtp"
	"fmt"
	"bytes"
)

type Email struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	From     string `yaml:"from"`
	Password string `yaml:"password"`
}

func (e *Email) Send(to []string, subject string, body []byte, contentType, contentEncoding string) error {
	buf := &bytes.Buffer{}
	buf.WriteString("Subject: " + subject + "\r\n")
	if contentType != "" {
		buf.WriteString("Content-Type: " + contentType + "\r\n")
	}
	if contentEncoding != "" {
		buf.WriteString("Content-Encoding: " + contentEncoding + "\r\n")
	}
	buf.WriteString("\r\n")
	buf.Write(body)
	return smtp.SendMail(fmt.Sprintf("%v:%v", e.Host, e.Port),
		smtp.PlainAuth("", e.User, e.Password, e.Host),
		e.From,
		to, buf.Bytes())
}

type Config struct {
	Reader common.Reader `yaml:"reader"`
	Mail   Email  `yaml:"mail"`
}

func main() {
	var app = kingpin.New("amqp-email-output", "Push data to AMQP broker and wait for response. Expects headers 'to' and 'subject'")
	var (
		quiet  = app.Flag("quiet", "Disable verbose logging").Short('q').Bool()
		config = app.Flag("config", "Configuration .YAML file").Short('c').Default("email.yaml").ExistingFile()
	)
	app.DefaultEnvars()
	kingpin.MustParse(app.Parse(os.Args[1:]))
	if *quiet {
		log.SetOutput(ioutil.Discard)
	} else {
		log.SetOutput(os.Stderr)
	}

	var props Config
	common.MustRead(*config, &props)

	err := props.Reader.Consume(false, func(deliveries <-chan amqp.Delivery) error {

		for msg := range deliveries {
			if msg.Headers == nil {
				msg.Nack(false, false)
				log.Println("Message", msg.MessageId, "has no headers")
				continue
			}
			to, ok := msg.Headers["to"]
			if !ok {
				msg.Nack(false, false)
				log.Println("Message", msg.MessageId, "has no 'to' header")
				continue
			}
			var dest []string

			if toS, ok := to.(string); ok {
				dest = []string{toS}
			} else if toS, ok := to.([]string); ok {
				dest = toS
			} else {
				msg.Nack(false, false)
				log.Println("Message", msg.MessageId, "has non-string (or non-array-of-string) 'to' header")
				continue
			}
			title, ok := msg.Headers["subject"]
			if !ok {
				msg.Nack(false, false)
				log.Println("Message", msg.MessageId, "has no 'subject' header")
				continue
			}
			titleS, ok := title.(string)
			if !ok {
				msg.Nack(false, false)
				log.Println("Message", msg.MessageId, "has non-string 'subject' header")
				continue
			}

			log.Println("Sending...")
			err := props.Mail.Send(dest, titleS, msg.Body, msg.ContentType, msg.ContentEncoding)
			if err != nil {
				log.Println("Failed send", err)
				msg.Nack(false, true)
				continue
			}
			log.Println("Sent")
			err = msg.Ack(false)
			if err != nil {
				log.Println("Failed ack", err)
				return err
			}

		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
