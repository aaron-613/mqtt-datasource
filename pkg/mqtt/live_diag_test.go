package mqtt

import (
	"context"
	"encoding/base64"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// TestLive_Diagnostic exercises the real EnsureTopic -> Subscribe -> StreamDelta path
// against a live broker (start scripts/statspump_broker.js on :1884 first).
// Guarded by MQTT_LIVE=1 so it never runs in CI.
func TestLive_Diagnostic(t *testing.T) {
	if os.Getenv("MQTT_LIVE") == "" {
		t.Skip("set MQTT_LIVE=1 and run a broker on :1884 to run this diagnostic")
	}
	uri := envOr("MQTT_URI", "tcp://localhost:1884")
	topic := envOr("MQTT_TOPIC", "pump/system/pollerA")
	fields := strings.Split(envOr("MQTT_FIELDS", "stats/totalTimeMs,stats/objectCount"), ",")

	ctx := context.Background()
	c, err := NewClient(ctx, Options{URI: uri}, backend.DataSourceInstanceSettings{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Dispose()

	b64 := base64.RawURLEncoding.EncodeToString([]byte(topic))
	tp := &Topic{
		Path:         b64,
		StreamingKey: "uid/hash/1",
		Interval:     time.Second,
		Fields:       fields,
	}
	stored := c.EnsureTopic(tp)
	key := stored.Key()
	t.Logf("key=%s fields=%v", key, stored.Fields)

	if _, err := c.Subscribe(key, log.DefaultLogger); err != nil {
		t.Fatal(err)
	}
	time.Sleep(8 * time.Second)

	got, ok := c.GetTopic(key)
	buf := got.stream.raw.snapshot()
	t.Logf("topic found=%v buffer_len=%d", ok, len(buf))
	if len(buf) > 0 {
		t.Logf("  sample raw payload: %s", string(buf[len(buf)-1].Value))
	}

	frame, hasData, err := got.StreamDelta(log.DefaultLogger)
	t.Logf("StreamDelta hasData=%v err=%v", hasData, err)
	if frame != nil {
		for _, f := range frame.Fields {
			var last interface{}
			if f.Len() > 0 {
				last = f.At(f.Len() - 1)
			}
			t.Logf("  field %q type=%s len=%d last=%v", f.Name, f.Type(), f.Len(), last)
		}
	}
	if !hasData {
		t.Fatal("expected streamed data but got none")
	}
}
