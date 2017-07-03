package main

import (
	"time"
	"github.com/streadway/amqp"
	"github.com/mcos/mapstructure"
)

type Endpoint struct {
	Exchange   string `mapstructure:"exchange"`
	RoutingKey string `mapstructure:"key"`
}

func (ep *Endpoint) encodeToMap() map[string]interface{} {
	// hack to wait for metchellh
	return map[string]interface{}{
		"exchange": ep.Exchange,
		"key":      ep.RoutingKey,
	}
}

func (ep *Endpoint) Send(msg *amqp.Publishing) error {
	return nil
}

type Transform struct {
	Endpoint                        `mapstructure:",squash"`
	Headers map[string]interface{}  `mapstructure:"headers"`
	Timeout time.Duration           `mapstructure:"timeout"`
}

func (ep *Transform) encodeToMap() map[string]interface{} {
	// hack to wait for metchellh
	exp := ep.Endpoint.encodeToMap()
	exp["headers"] = ep.Headers
	exp["timeout"] = int64(ep.Timeout)
	return exp
}

type Source struct {
	Name     string
	Bindings []Endpoint
	Durable  bool
	Lazy     bool
}

type Sink struct {
	Endpoint `mapstructure:",squash"`
}

type Flow struct {
	Name       string        `mapstructure:"x-flow-name"`
	Source     Source        `mapstructure:"-"`
	Transforms []Transform   `mapstructure:"x-flow-transforms"`
	Sink       Sink          `mapstructure:"x-flow-sink"`
}

func (fl *Flow) EncodeToMap() map[string]interface{} {
	var transforms []interface{}
	for _, t := range fl.Transforms {
		transforms = append(transforms, t.encodeToMap())
	}
	return map[string]interface{}{
		"x-flow-name":       fl.Name,
		"x-flow-sink":       fl.Sink.encodeToMap(),
		"x-flow-transforms": transforms,
	}

}

func (fl *Flow) DecodeFromMap(data amqp.Table) error {
	return mapstructure.Decode(data, fl)
}
