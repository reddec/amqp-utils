package common

import (
	"encoding/json"
	"strconv"
	"time"
)

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

func (m *Message) Columns() ([]string, error) {
	data, err := json.Marshal(m.Headers)

	return []string{time.Now().Format(time.RFC3339Nano),
		m.MessageId,
		string(data),
		m.Body,
		m.ContentType,
		m.ContentEncoding,
		strconv.FormatUint(uint64(m.DeliveryMode), 10),
		strconv.FormatUint(uint64(m.Priority), 10),
		m.CorrelationId,
		m.ReplyTo,
		m.Expiration,
		m.Timestamp.Format(time.RFC3339Nano),
		m.Type,
		m.Exchange,
		m.RoutingKey}, err
}

func MessageHeaders() []string {
	return []string{"stamp",
		"messageid",
		"headers",
		"body",
		"contenttype",
		"contentencoding",
		"deliverymode",
		"priority",
		"correlationid",
		"replyto",
		"expiration",
		"timestamp",
		"type",
		"exchange",
		"routingkey"}
}
