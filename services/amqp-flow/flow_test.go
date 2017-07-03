package main

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
)

func TestFlow_EncodeToMap(t *testing.T) {
	var flow Flow
	flow.Name = "Sample"
	flow.Sink.RoutingKey = "data.done"
	flow.Sink.Exchange = "amq.topic"
	flow.Source.Name = "data.in"
	flow.Source.Durable = true
	flow.Source.Lazy = false
	flow.Transforms = append(flow.Transforms, Transform{
		Timeout: 30 * time.Second,
		Endpoint: Endpoint{
			Exchange:   "tr1",
			RoutingKey: "ready",
		},
		Headers: map[string]interface{}{
			"h1": "v1",
		},
	})

	data := flow.EncodeToMap()

	assert.EqualValues(t, map[string]interface{}{
		"x-flow-name": "Sample",
		"x-flow-sink": map[string]interface{}{
			"key":      "data.done",
			"exchange": "amq.topic",
		},
		"x-flow-transforms": []interface{}{
			map[string]interface{}{
				"key":      "ready",
				"exchange": "tr1",
				"timeout":  int64(30000000000),
				"headers": map[string]interface{}{
					"h1": "v1",
				},
			},
		},
	}, data)

}

func TestFlow_DecodeFromMap(t *testing.T) {
	var sample Flow
	sample.Name = "Sample"
	sample.Sink.RoutingKey = "data.done"
	sample.Sink.Exchange = "amq.topic"
	sample.Transforms = append(sample.Transforms, Transform{
		Timeout: 30 * time.Second,
		Endpoint: Endpoint{
			Exchange:   "tr1",
			RoutingKey: "ready",
		},
		Headers: map[string]interface{}{
			"h1": "v1",
		},
	})

	data := map[string]interface{}{
		"x-flow-name": "Sample",
		"x-flow-sink": map[string]interface{}{
			"key":      "data.done",
			"exchange": "amq.topic",
		},
		"x-flow-transforms": []interface{}{
			map[string]interface{}{
				"key":      "ready",
				"exchange": "tr1",
				"timeout":  int64(30000000000),
				"headers": map[string]interface{}{
					"h1": "v1",
				},
			},
		},
	}

	var flow Flow
	assert.NoError(t, flow.DecodeFromMap(data))
	assert.EqualValues(t, sample, flow)
}
