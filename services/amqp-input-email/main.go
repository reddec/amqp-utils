package main

import (
	"github.com/emersion/go-imap/client"
	"../../common"
	"log"
	"github.com/emersion/go-imap"
	"github.com/streadway/amqp"
	"time"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"os"
	"github.com/alecthomas/kingpin"
	"net/mail"
)

type IMAP struct {
	Server    string `yaml:"server"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Mailbox   string `yaml:"mailbox"`
	TLS       bool   `yaml:"tls"`
	Interval  time.Duration `yaml:"interval"`
	Reconnect common.Reconnect `yaml:"reconnect"`
}

func convertAddress(list []*imap.Address) []interface{} {
	var res []interface{}
	for _, i := range list {
		res = append(res, i.MailboxName+"@"+i.HostName)
	}
	return res
}

func (im *IMAP) onConection(conn *client.Client, writer *common.Writer) error {
	defer conn.Close()
	log.Println("Logging...")
	err := conn.Login(im.User, im.Password)
	if err != nil {
		return err
	}
	log.Println("Successfully logged in")
	defer conn.Logout()

	for {
		log.Println("Openning", im.Mailbox)
		mbox, err := conn.Select(im.Mailbox, false)
		if err != nil {
			return err
		}
		log.Println("Opened", mbox.Name, "-", mbox.Unseen, "new messages")
		search := imap.NewSearchCriteria()
		search.WithoutFlags = append(search.WithoutFlags, imap.SeenFlag)
		result, err := conn.Search(search)
		if err != nil {
			return err
		}
		log.Println("Found", len(result), "new messages")
		if len(result) > 0 {
			log.Println("Fetching...")
			seqset := new(imap.SeqSet)
			seqset.AddNum(result...)
			messages := make(chan *imap.Message)
			done := make(chan error, 1)
			go func() {
				done <- conn.Fetch(seqset, []string{imap.EnvelopeMsgAttr, "BODY[]", imap.BodyStructureMsgAttr}, messages)
			}()
			for msg := range messages {
				log.Println(msg.Envelope.Subject)
				headers := map[string]interface{}{
					"from":  convertAddress(msg.Envelope.From),
					"to":    convertAddress(msg.Envelope.To),
					"stamp": msg.Envelope.Date,
				}

				m, err := mail.ReadMessage(msg.GetBody("BODY[]"))
				if err != nil {
					return err
				}
				cts := m.Header["Content-Type"]
				contentType := ""
				if len(cts) > 0 {
					contentType = cts[0]
				}
				body, err := ioutil.ReadAll(m.Body)
				if err != nil {
					log.Fatal(err)
				}
				msg := amqp.Publishing{
					Body:            body,
					Headers:         headers,
					Timestamp:       time.Now(),
					MessageId:       uuid.NewV4().String(),
					AppId:           "email",
					ContentType:     contentType,
					ContentEncoding: msg.BodyStructure.Encoding,
				}
				writer.Write(msg)
			}
			log.Println("Done fetching")
			if err := <-done; err != nil {
				return err
			}
			log.Println("Marks all as seen")
			operations := "+FLAGS.SILENT"
			if err := conn.Store(seqset, operations, []interface{}{imap.SeenFlag}, nil); err != nil {
				return err
			}
		} else {
			log.Println("Waiting", im.Interval)
			time.Sleep(im.Interval)
		}

	}

}

func (im *IMAP) Consume(writer *common.Writer) error {
	if im.Interval == 0 {
		im.Interval = 5 * time.Second
	}
	for {
		log.Println("Connecting to email server", im.Server)
		var conn *client.Client
		var err error
		if im.TLS {
			conn, err = client.DialTLS(im.Server, nil)
		} else {
			conn, err = client.Dial(im.Server)
		}
		if err != nil {
			log.Println("Failed connect to email server due to", err)
		} else {
			err = im.onConection(conn, writer)
		}
		if err != nil {
			log.Println("Read error", err)
		}
		if im.Reconnect.Disable {
			return err
		}
		log.Println("Wait...")
		im.Reconnect.Wait()
	}
}

type Config struct {
	Writer common.Writer `yaml:"writer"`
	Mail   IMAP  `yaml:"imap"`
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
	props.Writer.Init()

	go func() {
		defer props.Writer.Close()
		props.Mail.Consume(&props.Writer)
	}()

	err := props.Writer.Run()
	if err != nil {
		log.Fatal(err)
	}
}
