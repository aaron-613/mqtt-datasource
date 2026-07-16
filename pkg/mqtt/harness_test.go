package mqtt

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/stretchr/testify/require"
)

// Deterministic in-process harness over the REAL *client + fakePaho. It drives the full
// lifecycle (QueryData -> RunStream(Subscribe) -> broker messages -> Unsubscribe -> janitor)
// so the interaction matrix — where the subtle bugs live — is exercised without a broker.
//
// publish() models how a real broker delivers to paho: an exact-match explicit route wins, else
// the single default (nil-callback) handler fires — i.e. a message is delivered ONCE per
// connection even when it matches several (overlapping) subscriptions. This is the assumption the
// wildcard/coverage code relies on, so the harness encodes it explicitly.
type harness struct {
	t  *testing.T
	c  *client
	fp *fakePaho
}

func newHarness(t *testing.T) *harness {
	fp := &fakePaho{}
	return &harness{t: t, c: newWildcardTestClient(fp), fp: fp}
}

// publish delivers a message the broker way: to the concrete topic's explicit route if one is
// installed, otherwise to the default handler (dispatch). Exactly one handler fires.
func (h *harness) publish(topic string, payload []byte) {
	if cb := h.fp.routes[topic]; cb != nil {
		cb(nil, fakeMessage{topic: topic, payload: payload})
		return
	}
	h.c.dispatch(nil, fakeMessage{topic: topic, payload: payload})
}

// queryConcrete models QueryData for a concrete topic: EnsureTopic + a seed frame.
func (h *harness) queryConcrete(topic, streamingKey string, fields ...string) *Topic {
	return h.c.EnsureTopic(&Topic{
		Path: encodeTopic(topic), StreamingKey: streamingKey, Interval: time.Second, Fields: fields,
	})
}

func (h *harness) reqPath(topic, streamingKey string) string {
	return "1s/" + encodeTopic(topic) + "/" + streamingKey
}

func (h *harness) subscribe(topic, streamingKey string) *Topic {
	top, err := h.c.Subscribe(h.reqPath(topic, streamingKey), log.DefaultLogger)
	require.NoError(h.t, err)
	return top
}

func (h *harness) unsubscribe(topic, streamingKey string) {
	require.NoError(h.t, h.c.Unsubscribe(h.reqPath(topic, streamingKey), log.DefaultLogger))
}

func (h *harness) brokerSubs() []string { return h.fp.subscribed }

// TestHarness_ReconnectBeforeQueryData covers the Grafana-Live race the fallback exists for:
// RunStream re-establishes a channel (Subscribe -> a degraded, field-less classic view) BEFORE
// QueryData runs EnsureTopic with the real field selection. The buffer must survive and the
// framer must be upgraded in place when the fields arrive (no blank, no restart).
func TestHarness_ReconnectBeforeQueryData(t *testing.T) {
	h := newHarness(t)

	// Live reconnects first: a degraded classic view + its own subscription.
	h.subscribe("a/b", "k")
	require.Equal(t, []string{"a/b"}, h.brokerSubs())
	h.publish("a/b", []byte(`{"v":1}`))
	h.publish("a/b", []byte(`{"v":2}`))

	// QueryData now arrives WITH a field selection -> reconcile upgrades the framer, buffer kept.
	top := h.queryConcrete("a/b", "k", "v")
	h.publish("a/b", []byte(`{"v":3}`))

	seed, err := top.SeedFrame(log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, 3, seed.Fields[0].Len(), "history survived the reconnect-before-QueryData upgrade")
	require.Equal(t, "v", seed.Fields[1].Name)
	require.Equal(t, data.FieldTypeNullableFloat64, seed.Fields[1].Type(), "framer upgraded to the selected field")
}

// TestHarness_ReapReleasesBrokerSub covers the full idle-to-reap lifecycle through the harness:
// once the last consumer detaches and the grace elapses, the janitor unsubscribes the broker and
// frees the raw.
func TestHarness_ReapReleasesBrokerSub(t *testing.T) {
	h := newHarness(t)
	h.c.gracePeriod = time.Millisecond

	h.queryConcrete("a/b", "k")
	h.subscribe("a/b", "k")
	h.publish("a/b", []byte(`{"v":1}`))
	h.unsubscribe("a/b", "k")

	time.Sleep(2 * time.Millisecond)
	h.c.sweep()

	require.Equal(t, []string{"a/b"}, h.fp.unsubscribed, "reap releases the broker subscription")
	h.c.rawMu.RLock()
	_, ok := h.c.raws[encodeTopic("a/b")]
	h.c.rawMu.RUnlock()
	require.False(t, ok, "reaped raw is removed from the map")
}

// TestHarness_ConcreteAndWildcardOverlap_SingleDelivery covers a topic graphed BOTH by a concrete
// panel and by a matching wildcard panel. The concrete panel keeps its own subscription (scoped
// coverage); a single broker delivery feeds the shared buffer exactly once (no double-append);
// both views see it; and the wildcard enumerates the topic even though a concrete route exists.
func TestHarness_ConcreteAndWildcardOverlap_SingleDelivery(t *testing.T) {
	h := newHarness(t)
	// A wildcard panel is active for a/+.
	h.c.wildcards["a/+"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

	// A concrete panel graphs a/b directly.
	h.queryConcrete("a/b", "kc")
	h.subscribe("a/b", "kc")
	require.Equal(t, []string{"a/b"}, h.brokerSubs(), "concrete panel opens its own sub, not absorbed by the wildcard")

	// The wildcard's own a/b series (enumerated by a/+) also views the same topic.
	h.c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), StreamingKey: "kw", Interval: time.Second, EnumeratedBy: "a/+"})
	wildView := h.subscribe("a/b", "kw")

	// One broker delivery (routed to the concrete's explicit handler) -> appended ONCE.
	h.publish("a/b", []byte(`{"v":1}`))

	require.Equal(t, 1, wildView.bufferLen(), "single delivery, single append visible to both views")
	require.Equal(t, []string{"a/b"}, h.c.wildcardSeen("a/+"), "wildcard enumerates the topic despite the concrete route")
	require.Len(t, h.brokerSubs(), 1, "still exactly one broker subscription for the topic")
}
