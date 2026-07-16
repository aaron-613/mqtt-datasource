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

func mkTopic(topicStr string, fields ...string) *Topic {
	return newStreamTopic(base64.RawURLEncoding.EncodeToString([]byte(topicStr)), time.Second, fields, time.Hour)
}

func TestSelection_ConcreteDefaultsToLastLevel(t *testing.T) {
	realTopic := "mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/stats_client"
	topic := mkTopic(realTopic, "stats.total-time-ms") // no label config -> default topic/blank
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"total-time-ms":2.5}}`)})

	frame, err := topic.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Empty(t, frame.Fields[0].Labels, "Time field should have no labels")
	require.Equal(t, realTopic, frame.Fields[1].Labels["topic"])
	// Opinionated default: blank topic level on a concrete topic = its last level.
	require.Equal(t, "stats_client", frame.Fields[1].Labels["series"])
	require.Equal(t, "stats_client", frame.Fields[1].Config.DisplayNameFromDS)
}

func TestSelection_AliasIsTheMetricInComposition(t *testing.T) {
	// A per-field alias renames the metric; it appears in the "object · metric" composition.
	topic := mkTopic("a/vpn3/c", "stats.a", "stats.b")
	topic.SeriesName = "vpn3" // as if wildcard-matched
	topic.FieldAliases = map[string]string{"stats.a": "Latency"}
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"a":1,"b":2}}`)})

	frame, err := topic.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, "vpn3 · Latency", frame.Fields[1].Config.DisplayNameFromDS)
	require.Equal(t, "vpn3 · b", frame.Fields[2].Config.DisplayNameFromDS)
}

func TestSelection_PayloadLabelSource(t *testing.T) {
	topic := mkTopic("t", "stats.v")
	topic.LabelSource = "payload"
	topic.LabelValue = "vpn-name"
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"vpn-name":"prod-vpn","stats":{"v":1.5}}`)})

	frame, err := topic.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	vf := frame.Fields[1]
	require.Equal(t, "prod-vpn", vf.Labels["series"])
	require.Equal(t, "prod-vpn", vf.Config.DisplayNameFromDS)
}

func TestSelection_TopicLevelLabel(t *testing.T) {
	topic := mkTopic("mqtt/PUMP/solace1025/POLLER_STAT/VPN/vpn3/queue_rates", "stats.v")
	topic.LabelSource = "topic"
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"v":1}}`)})

	topic.LabelValue = "5" // vpn3
	frame, _ := topic.SeedFrame(log.DefaultLogger)
	require.Equal(t, "vpn3", frame.Fields[1].Config.DisplayNameFromDS)
	require.Equal(t, "vpn3", frame.Fields[1].Labels["series"])

	topic.LabelValue = "2,-1" // solace1025 + queue_rates (negative index = from end)
	frame, _ = topic.SeedFrame(log.DefaultLogger)
	require.Equal(t, "solace1025_queue_rates", frame.Fields[1].Config.DisplayNameFromDS)
}

func TestSelection_CustomLabel(t *testing.T) {
	topic := mkTopic("a/b", "stats.v")
	topic.LabelSource = "custom"
	topic.LabelValue = "MyLabel"
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"v":1}}`)})

	frame, _ := topic.SeedFrame(log.DefaultLogger)
	require.Equal(t, "MyLabel", frame.Fields[1].Config.DisplayNameFromDS)
}

func TestSelection_MultiFieldComposition(t *testing.T) {
	// Wildcard-matched object + two metrics -> "object · metric" per series.
	topic := mkTopic("a/vpn3/c", "stats.a", "stats.b")
	topic.SeriesName = "vpn3" // as if set by the wildcard expansion
	topic.appendMessage(Message{Timestamp: time.Now(), Value: []byte(`{"stats":{"a":1,"b":2}}`)})

	frame, _ := topic.SeedFrame(log.DefaultLogger)
	require.Equal(t, "vpn3 · a", frame.Fields[1].Config.DisplayNameFromDS)
	require.Equal(t, "vpn3 · b", frame.Fields[2].Config.DisplayNameFromDS)
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

func TestRawTopic_Since(t *testing.T) {
	base := time.Now()
	r := newRawTopic("p", time.Hour)
	// Ascending timestamps; two share base+2s to exercise the equal-timestamp boundary.
	for i, tm := range []time.Time{
		base, base.Add(time.Second), base.Add(2 * time.Second), base.Add(2 * time.Second), base.Add(3 * time.Second),
	} {
		r.append(Message{Timestamp: tm, Value: []byte{byte(i)}})
	}

	require.Len(t, r.since(base.Add(-time.Second)), 5, "watermark before all -> all")
	require.Len(t, r.since(base), 4, "strictly-after excludes the base message")
	require.Len(t, r.since(base.Add(2*time.Second)), 1, "excludes BOTH equal-timestamp messages")
	require.Empty(t, r.since(base.Add(3*time.Second)), "watermark == last -> none")
	require.Empty(t, r.since(base.Add(time.Hour)), "watermark after all -> none")
	require.Empty(t, newRawTopic("q", time.Hour).since(base), "empty buffer -> none")
}

func TestTopic_RingBuffer_TrimsByWindow(t *testing.T) {
	base := time.Now()
	topic := newStreamTopic("dGVzdA", time.Second, nil, 10*time.Second)

	// Three messages spanning 20s; the oldest falls outside a 10s window.
	topic.appendMessage(Message{Timestamp: base.Add(-20 * time.Second), Value: []byte("1")})
	topic.appendMessage(Message{Timestamp: base.Add(-5 * time.Second), Value: []byte("2")})
	topic.appendMessage(Message{Timestamp: base, Value: []byte("3")})

	require.Equal(t, 2, topic.bufferLen(), "oldest message should be trimmed out of the window")
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

func TestSharedRawBuffer_TwoSelectionsOneTopic(t *testing.T) {
	c := &client{discovered: make(map[string][]byte)}
	// Two panels on the same MQTT topic (same Path) but different selections/keys, both
	// registered via EnsureTopic so they share the topic's one raw buffer.
	sel := c.EnsureTopic(&Topic{Path: "cG9sbGVy", StreamingKey: "uid/aaa/1", Interval: time.Second, Fields: []string{"stats.totalTimeMs"}})
	classic := c.EnsureTopic(&Topic{Path: "cG9sbGVy", StreamingKey: "uid/bbb/1", Interval: time.Second})

	// A single delivery into the shared raw buffer is visible to both views (no fan-out copy).
	c.HandleMessage("cG9sbGVy", []byte(`{"stats":{"totalTimeMs":1}}`))

	require.Equal(t, 1, sel.bufferLen(), "selection view should see the message")
	require.Equal(t, 1, classic.bufferLen(), "classic view on the same MQTT topic should also see it")
	require.Len(t, c.raws, 1, "exactly one shared raw buffer for the MQTT topic")
}

func TestEnsureTopic_NewSelectionSharesHistory(t *testing.T) {
	c := &client{discovered: make(map[string][]byte)}
	a := c.EnsureTopic(&Topic{Path: "dGVzdA", StreamingKey: "uid/aaa/1", Interval: time.Second, Fields: []string{"x"}})
	c.HandleMessage("dGVzdA", []byte(`{"x":1}`))
	c.HandleMessage("dGVzdA", []byte(`{"x":2}`))
	require.Equal(t, 2, a.bufferLen())

	// Adding a 2nd field changes the streaming key -> a new view on the same MQTT path. It
	// shares the existing raw buffer, so it opens with history instead of blanking (no copy).
	b := c.EnsureTopic(&Topic{Path: "dGVzdA", StreamingKey: "uid/bbb/1", Interval: time.Second, Fields: []string{"x", "y"}})
	require.NotSame(t, a, b, "distinct view instances")
	require.Equal(t, 2, b.bufferLen(), "new field selection shares the topic's existing history")
	require.Len(t, c.raws, 1, "both selections share one raw buffer")
}

func TestEnsureTopic_AddFieldReseedsWithHistoryNoBlank(t *testing.T) {
	c := &client{discovered: make(map[string][]byte)}
	// Panel A graphs field x; history accumulates on the shared raw buffer.
	a := c.EnsureTopic(&Topic{Path: "dGVzdA", StreamingKey: "uid/aaa/1", Interval: time.Second, Fields: []string{"x"}})
	c.HandleMessage("dGVzdA", []byte(`{"x":1,"y":2}`))
	c.HandleMessage("dGVzdA", []byte(`{"x":3,"y":4}`))
	seedA, err := a.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 2, seedA.Fields[0].Len())

	// Adding field y (a new streaming key) opens a new view that immediately reseeds the shared
	// history through its OWN framer — no blank, no restart, and no missing-number-field.
	b := c.EnsureTopic(&Topic{Path: "dGVzdA", StreamingKey: "uid/bbb/1", Interval: time.Second, Fields: []string{"x", "y"}})
	seedB, err := b.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 3, len(seedB.Fields), "Time + x + y")
	require.Equal(t, 2, seedB.Fields[0].Len(), "reseeds the existing 2 rows of history")
	require.Equal(t, "y", seedB.Fields[2].Name)
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
	c := &client{raws: map[string]*rawTopic{}}

	// Each scenario is a distinct MQTT topic (Path) = its own shared raw. Reaping is decided on
	// the raw (refCount + detachedAt); the raw's views are deleted with it.
	mk := func(topic, key string, refCount int, detachedAt time.Time) *Topic {
		path := encodeTopic(topic)
		raw := newRawTopic(path, time.Hour)
		raw.refCount = refCount
		raw.detachedAt = detachedAt
		c.raws[path] = raw
		top := newStreamTopic(path, time.Second, nil, time.Hour)
		top.StreamingKey = key
		top.stream.raw = raw
		c.topics.Store(top)
		return top
	}

	// refCount 0, detached past grace, no live sub -> swept.
	stale := mk("stale", "s/stale", 0, time.Now().Add(-2*defaultGracePeriod))
	// refCount 0, detached within grace (e.g. mid-zoom) -> retained.
	fresh := mk("fresh", "s/fresh", 0, time.Now())
	// Still attached (refCount 1) -> retained regardless of age.
	attached := mk("attached", "s/attached", 1, time.Now().Add(-2*defaultGracePeriod))

	c.sweep()

	_, staleKept := c.topics.Load(stale.Key())
	_, freshKept := c.topics.Load(fresh.Key())
	_, attachedKept := c.topics.Load(attached.Key())
	require.False(t, staleKept, "stale detached topic should be swept")
	require.True(t, freshKept, "recently-detached topic should be retained for zoom reseed")
	require.True(t, attachedKept, "attached topic should never be swept")

	// The stale topic's shared raw buffer is freed too.
	_, staleRaw := c.raws[encodeTopic("stale")]
	require.False(t, staleRaw, "stale raw buffer should be freed on reap")
}
