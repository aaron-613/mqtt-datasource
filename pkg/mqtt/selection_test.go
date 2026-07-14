package mqtt

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/stretchr/testify/require"
)

// StatsPump-shaped payload: metrics nested under "stats".
const statsPumpMsg = `{"hostname":"host1","name":"poller-meta","stats":{"totalTimeMs":12.5,"objectCount":42}}`

func TestFramer_Selection_NestedPaths(t *testing.T) {
	f := newFramer("stats.totalTimeMs", "stats.objectCount")
	msgs := []Message{
		{Timestamp: time.Unix(0, 0), Value: []byte(statsPumpMsg)},
		{Timestamp: time.Unix(1, 0), Value: []byte(`{"stats":{"totalTimeMs":13.0,"objectCount":43}}`)},
	}
	frame, err := f.toFrame(msgs, log.DefaultLogger)
	require.NoError(t, err)

	// Time + 2 selected columns, deterministic order.
	require.Equal(t, 3, len(frame.Fields))
	require.Equal(t, "Time", frame.Fields[0].Name)
	require.Equal(t, "stats.totalTimeMs", frame.Fields[1].Name)
	require.Equal(t, "stats.objectCount", frame.Fields[2].Name)
	require.Equal(t, data.FieldTypeNullableFloat64, frame.Fields[1].Type())
	require.Equal(t, 2, frame.Fields[1].Len())

	got := frame.Fields[1].At(0).(*float64)
	require.NotNil(t, got)
	require.Equal(t, 12.5, *got)
}

func TestSelection_AppliesTopicAndNameLabels(t *testing.T) {
	realTopic := "mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/stats_client"
	topic := newStreamTopic(base64.RawURLEncoding.EncodeToString([]byte(realTopic)), time.Second, []string{"stats.total-time-ms"}, time.Hour)
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"hostname":"solace1025","name":"show stats client","stats":{"total-time-ms":2.5}}`)})

	frame, err := topic.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 2, len(frame.Fields))
	require.Empty(t, frame.Fields[0].Labels, "Time field should have no labels")
	require.Equal(t, realTopic, frame.Fields[1].Labels["topic"])
	require.Equal(t, "show stats client", frame.Fields[1].Labels["name"])
}

func TestSelection_AppliesFieldAlias(t *testing.T) {
	topic := newStreamTopic(base64.RawURLEncoding.EncodeToString([]byte("t")), time.Second, []string{"stats.total-time-ms"}, time.Hour)
	topic.FieldAliases = map[string]string{"stats.total-time-ms": "Poll time"}
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"total-time-ms":2.5}}`)})

	frame, err := topic.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.NotNil(t, frame.Fields[1].Config)
	require.Equal(t, "Poll time", frame.Fields[1].Config.DisplayNameFromDS)
}

func TestFramer_Selection_MissingPathIsNil(t *testing.T) {
	f := newFramer("stats.totalTimeMs", "stats.missing")
	msgs := []Message{{Timestamp: time.Unix(0, 0), Value: []byte(statsPumpMsg)}}
	frame, err := f.toFrame(msgs, log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 3, len(frame.Fields))
	// Missing path yields a nil value, not a dropped row.
	require.Nil(t, frame.Fields[2].At(0))
	require.Equal(t, 1, frame.Fields[2].Len())
}

func TestFlattenLeafPaths(t *testing.T) {
	paths, err := FlattenLeafPaths([]byte(statsPumpMsg))
	require.NoError(t, err)
	require.Equal(t, []string{"hostname", "name", "stats.objectCount", "stats.totalTimeMs"}, paths)
}

func TestTopic_RingBuffer_TrimsByWindow(t *testing.T) {
	base := time.Now()
	topic := newStreamTopic("dGVzdA", time.Second, nil, 10*time.Second)

	// Three messages spanning 20s; the oldest falls outside a 10s window.
	topic.appendMessage(Message{Timestamp: base.Add(-20 * time.Second), Value: []byte("1")})
	topic.appendMessage(Message{Timestamp: base.Add(-5 * time.Second), Value: []byte("2")})
	topic.appendMessage(Message{Timestamp: base, Value: []byte("3")})

	require.Equal(t, 2, len(topic.Messages), "oldest message should be trimmed out of the window")
}

func TestTopic_SeedThenStream_NoDuplicates(t *testing.T) {
	base := time.Now()
	topic := newStreamTopic("dGVzdA", time.Second, []string{"v"}, time.Hour)

	topic.appendMessage(Message{Timestamp: base.Add(-2 * time.Second), Value: []byte(`{"v":1}`)})
	topic.appendMessage(Message{Timestamp: base.Add(-1 * time.Second), Value: []byte(`{"v":2}`)})

	// Seed returns both buffered rows and advances the watermark.
	seed, err := topic.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 2, seed.Fields[0].Len())

	// A new message arrives; the delta should contain ONLY the new row (no re-send).
	topic.appendMessage(Message{Timestamp: base, Value: []byte(`{"v":3}`)})
	delta, hasData, err := topic.StreamDelta(log.DefaultLogger)
	require.NoError(t, err)
	require.True(t, hasData)
	require.Equal(t, 1, delta.Fields[0].Len())

	// Nothing new -> no data.
	_, hasData, err = topic.StreamDelta(log.DefaultLogger)
	require.NoError(t, err)
	require.False(t, hasData)
}

func TestTopicMap_AddMessage_FansOutToSameTopic(t *testing.T) {
	var tm TopicMap
	// Two panels on the same MQTT topic (same Path) but different selections/keys.
	sel := newStreamTopic("cG9sbGVy", time.Second, []string{"stats.totalTimeMs"}, time.Hour)
	sel.StreamingKey = "uid/aaa/1"
	classic := newStreamTopic("cG9sbGVy", time.Second, nil, time.Hour)
	classic.StreamingKey = "uid/bbb/1"
	tm.Store(sel)
	tm.Store(classic)

	tm.AddMessage("cG9sbGVy", Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"totalTimeMs":1}}`)})

	require.Equal(t, 1, len(sel.Messages), "selection topic should receive the message")
	require.Equal(t, 1, len(classic.Messages), "classic topic on the same MQTT topic should also receive it")
}

func TestEnsureTopic_ReconcilesFields(t *testing.T) {
	c := &client{}
	// Simulate a classic (field-less) topic created by the RunStream fallback.
	classic := newStreamTopic("dGVzdA", time.Second, nil, time.Hour)
	classic.StreamingKey = "uid/h/1"
	c.topics.Store(classic)

	// QueryData arrives for the same key WITH a field selection.
	reconciled := c.EnsureTopic(&Topic{
		Path: "dGVzdA", StreamingKey: "uid/h/1", Interval: time.Second,
		Fields: []string{"stats.totalTimeMs"},
	})
	require.Same(t, classic, reconciled, "should reuse the existing topic instance")
	require.Equal(t, []string{"stats.totalTimeMs"}, reconciled.Fields)

	// The framer now emits the selected numeric column instead of classic extraction.
	reconciled.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"totalTimeMs":9.5}}`)})
	frame, err := reconciled.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 2, len(frame.Fields)) // Time + stats.totalTimeMs
	require.Equal(t, "stats.totalTimeMs", frame.Fields[1].Name)
	require.Equal(t, data.FieldTypeNullableFloat64, frame.Fields[1].Type())
}

func TestClient_Janitor_RetainsRecentDropsStale(t *testing.T) {
	c := &client{}

	// Detached longer than the grace period, no live MQTT sub -> should be swept.
	stale := newStreamTopic("dGVzdA", time.Second, nil, time.Hour)
	stale.StreamingKey = "s/stale"
	stale.stream.detachedAt = time.Now().Add(-2 * detachGracePeriod)
	c.topics.Store(stale)

	// Detached but within grace (e.g. mid-zoom) -> must be retained.
	fresh := newStreamTopic("dGVzdA", time.Second, nil, time.Hour)
	fresh.StreamingKey = "s/fresh"
	fresh.stream.detachedAt = time.Now()
	c.topics.Store(fresh)

	// Still attached (active panel) -> must be retained regardless of age.
	attached := newStreamTopic("dGVzdA", time.Second, nil, time.Hour)
	attached.StreamingKey = "s/attached"
	attached.stream.attachedCount = 1
	c.topics.Store(attached)

	c.sweep()

	_, staleKept := c.topics.Load(stale.Key())
	_, freshKept := c.topics.Load(fresh.Key())
	_, attachedKept := c.topics.Load(attached.Key())
	require.False(t, staleKept, "stale detached topic should be swept")
	require.True(t, freshKept, "recently-detached topic should be retained for zoom reseed")
	require.True(t, attachedKept, "attached topic should never be swept")
}
