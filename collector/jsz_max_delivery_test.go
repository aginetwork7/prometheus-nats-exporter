// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	highestDeliveryCountMetric     = "jetstream_consumer_highest_delivery_count"
	highestDeliveryStreamSeqMetric = "jetstream_consumer_highest_delivery_stream_seq"
)

// jszFixture serves a /varz and a /jsz response with a single account, stream
// and consumer. consumerExtra is spliced into the consumer info JSON.
func jszFixture(t *testing.T, consumerExtra string) string {
	t.Helper()
	jsz := fmt.Sprintf(`{
		"config": {},
		"account_details": [{
			"name": "A", "id": "ACC",
			"stream_detail": [{
				"name": "S",
				"state": {},
				"consumer_detail": [{
					"stream_name": "S", "name": "C",
					"delivered": {"consumer_seq": 120, "stream_seq": 99},
					"ack_floor": {"consumer_seq": 100, "stream_seq": 90},
					"num_ack_pending": 3, "num_redelivered": 2, "num_pending": 0%s
				}]
			}]
		}]
	}`, consumerExtra)

	mux := http.NewServeMux()
	mux.HandleFunc("/varz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"server_name": "srv"}`)
	})
	mux.HandleFunc("/jsz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, jsz)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// collectJszMetrics runs a full collection and returns every gauge value by
// metric name. Unlike verifySpecificCollector it waits for Collect to return,
// so a metric that is never emitted is simply missing from the result.
func collectJszMetrics(t *testing.T, url string) map[string][]float64 {
	t.Helper()
	coll := NewJszCollector("all", "", []*CollectedServer{{ID: "id", URL: url}}, []string{}, []string{})
	ch := make(chan prometheus.Metric)
	go func() {
		coll.Collect(ch)
		close(ch)
	}()
	got := make(map[string][]float64)
	for metric := range ch {
		pb := &dto.Metric{}
		if err := metric.Write(pb); err != nil {
			t.Fatalf("Unable to write metric: %v", err)
		}
		name := parseDesc(metric.Desc().String())
		got[name] = append(got[name], pb.GetGauge().GetValue())
	}
	return got
}

func requireValues(t *testing.T, got map[string][]float64, name string, want ...float64) {
	t.Helper()
	vals, ok := got[name]
	if !ok {
		t.Fatalf("metric %s was not emitted", name)
	}
	if len(vals) != len(want) {
		t.Fatalf("metric %s: expected %v, got %v", name, want, vals)
	}
	for i := range want {
		if vals[i] != want[i] {
			t.Fatalf("metric %s: expected %v, got %v", name, want, vals)
		}
	}
}

func TestJetStreamHighestDeliveryMetrics(t *testing.T) {
	t.Run("reported by the server", func(t *testing.T) {
		url := jszFixture(t, `, "max_delivery_count": 7, "max_delivery_stream_seq": 42`)
		got := collectJszMetrics(t, url)
		requireValues(t, got, highestDeliveryCountMetric, 7)
		// The sequence of the redelivered message (42) is not the delivered
		// (99) nor the ack floor (90) stream sequence.
		requireValues(t, got, highestDeliveryStreamSeqMetric, 42)
		requireValues(t, got, "jetstream_consumer_delivered_stream_seq", 99)
		requireValues(t, got, "jetstream_consumer_ack_floor_stream_seq", 90)
		requireValues(t, got, "jetstream_consumer_num_redelivered", 2)
	})

	t.Run("reported as zero when nothing is redelivered", func(t *testing.T) {
		url := jszFixture(t, `, "max_delivery_count": 0, "max_delivery_stream_seq": 0`)
		got := collectJszMetrics(t, url)
		requireValues(t, got, highestDeliveryCountMetric, 0)
		requireValues(t, got, highestDeliveryStreamSeqMetric, 0)
	})

	t.Run("not reported by the server", func(t *testing.T) {
		url := jszFixture(t, ``)
		got := collectJszMetrics(t, url)
		// The collection worked, but there is nothing to say about delivery counts.
		requireValues(t, got, "jetstream_consumer_num_redelivered", 2)
		for _, name := range []string{highestDeliveryCountMetric, highestDeliveryStreamSeqMetric} {
			if vals, ok := got[name]; ok {
				t.Fatalf("metric %s must not be emitted for a server without the fields, got %v", name, vals)
			}
		}
	})
}
