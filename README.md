# amqp-utils
Toolbox on Golang to manipulate messages from amqp broker

## amqp-http-input

Listen HTTP endpoint and post message to AMQP broker. Message must be as described 
below. 

Auth included =)

## amqp-http-hook

Gets message from AMQP broker and post to remote HTTP endpoint. If no `--template` 
provided raw JSON view of message is used.

Retries/multiple URLs/auth included =)

## amqp-http-csv

Same as amqp-http-input but puts message into CSV (I don't remember why I did it).

Auth included =)

## amqp-cgi


Runs provided executable and puts into stdin content of message. Headers are putted
into environment variables (as in message described: e.x. `ContentType`).

Additional headers from message are putted with prefix `AMQP_`. For example: `AMQP_MY_HEADER`

Stdout sended as message to `ReplyTo` queue (if provided) with `CorrelationId` = `MessageId`

Retries included =)

## amqp-push

Very simple utility that writes lines from stdin as messages to amqp broker or whole
data as one packet if `--single` flag provided

# Install 

Use [binary builds](https://github.com/reddec/amqp-utils/releases) or build 
by yourself:

Assume that you defined `GOPATH/bin` into `PATH`

    go install github.com/reddec/amqp-utils/cmd/...


Also you may setup a service like this:


    [Unit]
	Description=AMQP utils - pull messages from broker and push to HTTP endpoint
	
	[Service]
	ExecStart=/usr/local/bin/amqp-http-hook
	RestartSec=5s
	
	[Install]
	WantedBy=multi-user.target


Just replace `Description` and `ExecStart` to description of service and absolute
path to required executable.

Install service by those command: 
    
	systemctl enable /path/to/file.service
	
And start by

	systemctl start file.service


# HTTP-HOOK

At fact, this is simple web hook, backed by AMQP broker

## Template

You may send message no in JSON by using Golang template: flag `--template <filename>`

Possible values in template is same as message fields

# Message

I am too lazy to describe all fields, so 

```go
type Message struct {
	Headers map[string]interface{} `json:"headers,omitempty"`
	Body    string                 `json:"body"`

	ContentType     string    `json:"content_type,omitempty"`     // MIME content type
	ContentEncoding string    `json:"content_encoding,omitempty"` // MIME content encoding
	DeliveryMode    uint8     `json:"delivery_mode,omitempty"`    // queue implemention use - non-persistent (1) or persistent (2)
	Priority        uint8     `json:"priority,omitempty"`         // queue implementation use - 0 to 9
	CorrelationId   string    `json:"correlation_id,omitempty"`   // application use - correlation identifier
	ReplyTo         string    `json:"reply_to,omitempty"`         // application use - address to to reply to (ex: RPC)
	Expiration      string    `json:"expiration,omitempty"`       // implementation use - message expiration spec
	MessageId       string    `json:"message_id,omitempty"`       // application use - message identifier
	Timestamp       time.Time `json:"timestamp,omitempty"`        // application use - message timestamp
	Type            string    `json:"type,omitempty"`             // application use - message type name
	Exchange        string    `json:"exchange,omitempty"`         // basic.publish exhange
	RoutingKey      string    `json:"routing_key,omitempty"`      // basic.publish routing key
}
```