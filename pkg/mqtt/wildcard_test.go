package mqtt

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/stretchr/testify/require"
)

// fakeMessage implements paho.Message so tests can deliver a message to the client's single
// default publish handler (dispatch), exactly as paho would for a nil-callback subscription.
type fakeMessage struct {
	topic   string
	payload []byte
}

func (m fakeMessage) Duplicate() bool   { return false }
func (m fakeMessage) Qos() byte         { return 0 }
func (m fakeMessage) Retained() bool    { return false }
func (m fakeMessage) Topic() string     { return m.topic }
func (m fakeMessage) MessageID() uint16 { return 0 }
func (m fakeMessage) Payload() []byte   { return m.payload }
func (m fakeMessage) Ack()              {}

// deliver simulates the broker delivering one message to the default handler.
func (c *client) deliver(topic string, payload []byte) {
	c.dispatch(nil, fakeMessage{topic: topic, payload: payload})
}

func newWildcardTestClient(fp *fakePaho) *client {
	return &client{
		client:     fp,
		done:       make(chan struct{}),
		discovered: make(map[string][]byte),
		wildcards:  make(map[string]*wildcardSub),
		raws:       make(map[string]*rawTopic),
	}
}

func TestWildcard_SingleSubscription(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	// Pre-seed the seen-set so EnsureWildcard doesn't block on the cold-start wait.
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{"a/b/c": {}}, seenOrder: []string{"a/b/c"}}

	require.Equal(t, []string{"a/b/c"}, c.EnsureWildcard("a/+/c"))
	require.Equal(t, []string{"a/+/c"}, fp.subscribed, "exactly one paho subscription for the pattern")

	// Idempotent: a second call does not open another subscription.
	require.Equal(t, []string{"a/b/c"}, c.EnsureWildcard("a/+/c"))
	require.Len(t, fp.subscribed, 1)
}

func TestWildcard_ColdStartReturnsOnFirstTopic(t *testing.T) {
	oldWait, oldPoll := wildcardColdStartWait, wildcardPollInterval
	wildcardColdStartWait, wildcardPollInterval = 500*time.Millisecond, 10*time.Millisecond
	defer func() { wildcardColdStartWait, wildcardPollInterval = oldWait, oldPoll }()

	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	go func() {
		time.Sleep(30 * time.Millisecond) // after EnsureWildcard has subscribed
		c.deliver("a/b/c", []byte(`{"x":1}`))
	}()
	require.Equal(t, []string{"a/b/c"}, c.EnsureWildcard("a/+/c"), "returns as soon as the first topic is seen")
}

func TestWildcard_ColdStartTimesOutEmpty(t *testing.T) {
	oldWait, oldPoll := wildcardColdStartWait, wildcardPollInterval
	wildcardColdStartWait, wildcardPollInterval = 80*time.Millisecond, 10*time.Millisecond
	defer func() { wildcardColdStartWait, wildcardPollInterval = oldWait, oldPoll }()

	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	require.Empty(t, c.EnsureWildcard("a/+/c"), "nothing delivered -> empty after the bounded wait")
}

func TestWildcard_FeedsBuffersAndSeen(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}
	top := c.EnsureTopic(&Topic{Path: encodeTopic("a/b/c"), Interval: time.Second, StreamingKey: "k"})

	c.deliver("a/b/c", []byte(`{"x":1}`))
	c.deliver("a/b/c", []byte(`{"x":2}`))

	require.Equal(t, []string{"a/b/c"}, c.wildcardSeen("a/+/c"))
	require.Equal(t, 2, top.bufferLen(), "the wildcard subscription feeds the per-series ring buffer")
}

func TestWildcard_CoverageAttachOnly(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

	// queryWildcard would tag the series with the enumerating pattern; coverage is scoped to it.
	c.EnsureTopic(&Topic{Path: encodeTopic("a/b/c"), Interval: time.Second, StreamingKey: "k", EnumeratedBy: "a/+/c"})
	reqPath := "1s/" + encodeTopic("a/b/c") + "/k"
	tp, err := c.Subscribe(reqPath, log.DefaultLogger)
	require.NoError(t, err)
	require.Empty(t, fp.subscribed, "a covered topic must NOT open its own subscription")

	raw := tp.stream.raw
	raw.mu.Lock()
	require.False(t, raw.pahoSubscribed)
	require.Equal(t, "a/+/c", raw.coveredByPattern)
	require.Equal(t, 1, raw.refCount)
	raw.mu.Unlock()
	require.Equal(t, 1, c.wildcards["a/+/c"].refCount)

	require.NoError(t, c.Unsubscribe(reqPath, log.DefaultLogger))
	require.Equal(t, 0, c.wildcards["a/+/c"].refCount, "unsubscribe releases the wildcard refcount")
}

// TestSubscribe_SharedSubRefcountAndSiblingSurvival guards the exact bug found in live testing:
// two panels (distinct streaming keys) on the SAME concrete topic must share ONE broker
// subscription, and detaching/reaping one must NOT tear out the subscription the other still
// needs. Before subscription ownership moved onto the shared raw, reaping the backgrounded
// sibling called Unsubscribe on the shared MQTT topic and starved the viewed panel.
func TestSubscribe_SharedSubRefcountAndSiblingSurvival(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)

	path := encodeTopic("x/y/z")
	req1 := "1s/" + path + "/k1"
	req2 := "1s/" + path + "/k2"

	_, err := c.Subscribe(req1, log.DefaultLogger)
	require.NoError(t, err)
	_, err = c.Subscribe(req2, log.DefaultLogger)
	require.NoError(t, err)

	// Exactly ONE broker subscription; refCount aggregates both views.
	require.Equal(t, []string{"x/y/z"}, fp.subscribed, "one shared broker subscription for both panels")
	raw := c.raws[path]
	require.NotNil(t, raw)
	require.Equal(t, 2, raw.refCount)

	// Detach one panel: the shared sub must stay open (the sibling still needs it).
	require.NoError(t, c.Unsubscribe(req1, log.DefaultLogger))
	require.Equal(t, 1, raw.refCount)
	require.Empty(t, fp.unsubscribed, "detaching one panel must not unsubscribe the shared topic")
	require.True(t, raw.pahoSubscribed, "shared subscription stays open while any view is attached")

	// The surviving panel still gets data through the shared raw.
	c.HandleMessage(path, []byte(`{"v":1}`))
	tp2, ok := c.GetTopic(req2)
	require.True(t, ok)
	require.Equal(t, 1, tp2.bufferLen(), "surviving panel still fed after its sibling detached")

	// Detach the last panel: refCount hits 0, now eligible for janitor reap.
	require.NoError(t, c.Unsubscribe(req2, log.DefaultLogger))
	require.Equal(t, 0, raw.refCount)
	require.False(t, raw.detachedAt.IsZero(), "raw marked idle once the last consumer detaches")
}

// TestSubscribe_ConcreteFeedsOverlappingWildcardSeenSet guards the overlap bug found in live
// testing: a concrete panel on a topic that a wildcard panel also covers. The concrete sub's
// explicit paho route shadows the default dispatch handler for that topic, so the concrete
// callback must itself record the topic into the overlapping wildcard's seen-set — otherwise the
// wildcard panel never enumerates a series for it.
func TestSubscribe_ConcreteFeedsOverlappingWildcardSeenSet(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	// A wildcard panel is active for a/+ ...
	c.wildcards["a/+"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

	// ... and a concrete panel subscribes to a/b, which the wildcard also covers.
	reqPath := "1s/" + encodeTopic("a/b") + "/k"
	_, err := c.Subscribe(reqPath, log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, []string{"a/b"}, fp.subscribed)

	// A broker message arrives on the concrete route (its explicit handler, not dispatch).
	fp.deliverRoute("a/b", []byte(`{"v":1}`))

	require.Equal(t, []string{"a/b"}, c.wildcardSeen("a/+"),
		"a concrete subscription must record its topic into an overlapping wildcard's seen-set")
	tp, ok := c.GetTopic(reqPath)
	require.True(t, ok)
	require.Equal(t, 1, tp.bufferLen(), "the shared buffer is still fed for the concrete view")
}

// TestSubscribe_ConcreteNotAbsorbedByOverlappingWildcard guards scoped coverage across the
// shared raw: a concrete panel on a topic a wildcard also covers must open its OWN subscription
// (never ride the wildcard firehose), regardless of attach order. A directly-typed topic stays
// decoupled so removing the wildcard panel can't leave a firehose feeding it.
func TestSubscribe_ConcreteNotAbsorbedByOverlappingWildcard(t *testing.T) {
	concrete := "1s/" + encodeTopic("a/b") + "/kc"
	wild := "1s/" + encodeTopic("a/b") + "/kw"

	t.Run("concrete first", func(t *testing.T) {
		fp := &fakePaho{}
		c := newWildcardTestClient(fp)
		c.wildcards["a/+"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

		c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), Interval: time.Second, StreamingKey: "kc"})
		_, err := c.Subscribe(concrete, log.DefaultLogger)
		require.NoError(t, err)
		c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), Interval: time.Second, StreamingKey: "kw", EnumeratedBy: "a/+"})
		_, err = c.Subscribe(wild, log.DefaultLogger)
		require.NoError(t, err)

		raw := c.raws[encodeTopic("a/b")]
		require.True(t, raw.pahoSubscribed, "concrete panel opens its own subscription")
		require.Equal(t, "", raw.coveredByPattern, "not riding the wildcard")
		require.Equal(t, []string{"a/b"}, fp.subscribed)
		require.Equal(t, 0, c.wildcards["a/+"].refCount, "wildcard sub not held by the concrete topic")
	})

	t.Run("wildcard first, concrete upgrades", func(t *testing.T) {
		fp := &fakePaho{}
		c := newWildcardTestClient(fp)
		c.wildcards["a/+"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

		c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), Interval: time.Second, StreamingKey: "kw", EnumeratedBy: "a/+"})
		_, err := c.Subscribe(wild, log.DefaultLogger)
		require.NoError(t, err)
		raw := c.raws[encodeTopic("a/b")]
		require.Equal(t, "a/+", raw.coveredByPattern, "initially rides the wildcard")
		require.Equal(t, 1, c.wildcards["a/+"].refCount)

		// The concrete panel arrives -> upgrade to an own sub and release the coverage.
		c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), Interval: time.Second, StreamingKey: "kc"})
		_, err = c.Subscribe(concrete, log.DefaultLogger)
		require.NoError(t, err)

		require.True(t, raw.pahoSubscribed, "upgraded to its own subscription")
		require.Equal(t, "", raw.coveredByPattern, "coverage released on upgrade")
		require.Equal(t, []string{"a/b"}, fp.subscribed)
		require.Equal(t, 0, c.wildcards["a/+"].refCount, "wildcard sub no longer held by this topic (no firehose coupling)")
	})
}

func TestWildcard_ClassicUncoveredOpensOwnSub(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	// No covering pattern: the classic single-topic path opens its own subscription (backward compat).
	reqPath := "1s/" + encodeTopic("x/y/z") + "/k"
	_, err := c.Subscribe(reqPath, log.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, []string{"x/y/z"}, fp.subscribed)
}

func TestWildcard_UnrelatedConcreteNotAbsorbed(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	// A broad wildcard subscription is active and WOULD match a/b/c...
	c.wildcards["a/#"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}
	// ...but an independent concrete panel (no EnumeratedBy) must NOT be absorbed into it — it
	// opens its own subscription and stays independent (no firehose coupling, order-independent).
	c.EnsureTopic(&Topic{Path: encodeTopic("a/b/c"), Interval: time.Second, StreamingKey: "k"})
	reqPath := "1s/" + encodeTopic("a/b/c") + "/k"
	tp, err := c.Subscribe(reqPath, log.DefaultLogger)
	require.NoError(t, err)

	require.Equal(t, []string{"a/b/c"}, fp.subscribed, "independent concrete panel opens its own sub despite a covering wildcard")
	raw := tp.stream.raw
	raw.mu.Lock()
	require.Equal(t, "", raw.coveredByPattern)
	require.True(t, raw.pahoSubscribed)
	raw.mu.Unlock()
	require.Equal(t, 0, c.wildcards["a/#"].refCount, "the broad wildcard sub is untouched by the concrete panel")
}

func TestWildcard_RefcountReap(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true, refCount: 1}

	// Still attached -> spared.
	c.sweepWildcards()
	require.Empty(t, fp.unsubscribed)

	// Detach and age past the grace period -> reaped.
	c.detachWildcard("a/+/c")
	c.wildMu.Lock()
	c.wildcards["a/+/c"].detachedAt = time.Now().Add(-2 * defaultGracePeriod)
	c.wildMu.Unlock()
	c.sweepWildcards()
	require.Equal(t, []string{"a/+/c"}, fp.unsubscribed)
	c.wildMu.RLock()
	_, ok := c.wildcards["a/+/c"]
	c.wildMu.RUnlock()
	require.False(t, ok, "reaped sub is removed from the registry")
}

func TestWildcard_ReapSparedOnReattach(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	// Idle and stale (would be a reap candidate)...
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true, detachedAt: time.Now().Add(-2 * defaultGracePeriod)}
	// ...but a channel re-attaches (bumps refCount, clears detachedAt) before the sweep.
	require.True(t, c.attachWildcardExact("a/+/c"))
	c.sweepWildcards()
	require.Empty(t, fp.unsubscribed, "a re-attached sub is not reaped")
}

func TestWildcard_OverlappingPatternsNoDoubleAppend(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}
	c.wildcards["a/b/#"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}
	top := c.EnsureTopic(&Topic{Path: encodeTopic("a/b/c"), Interval: time.Second, StreamingKey: "k"})

	// The single default handler fires once per message even though the topic matches BOTH
	// patterns -> the buffer is appended exactly once.
	c.deliver("a/b/c", []byte(`{"x":1}`))

	require.Equal(t, 1, top.bufferLen(), "no double-append across overlapping patterns")
	require.Equal(t, []string{"a/b/c"}, c.wildcardSeen("a/+/c"))
	require.Equal(t, []string{"a/b/c"}, c.wildcardSeen("a/b/#"))
}

func TestWildcard_SharedPatternFansOutToBothPanels(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.wildcards["a/+/c"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}
	// Two panels graph the same concrete topic (same Path, different streaming keys).
	t1 := c.EnsureTopic(&Topic{Path: encodeTopic("a/b/c"), Interval: time.Second, StreamingKey: "k1"})
	t2 := c.EnsureTopic(&Topic{Path: encodeTopic("a/b/c"), Interval: time.Second, StreamingKey: "k2"})

	c.deliver("a/b/c", []byte(`{"x":1}`))

	// Both panels (views) on the same concrete topic share one raw buffer, so a single
	// delivery is visible to each without any per-panel copy.
	for _, tp := range []*Topic{t1, t2} {
		require.Equal(t, 1, tp.bufferLen(), "one message is visible to each panel's view via the shared buffer")
	}
	require.Len(t, c.raws, 1, "both panels share a single raw buffer for the topic")
}

// Identical filter string used by BOTH discovery and a wildcard panel: the broker keeps one
// subscription per filter, so neither owner may unsubscribe it while the other is live.
func TestWildcard_DiscoveryReapKeepsSharedFilterForWildcard(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.discoveryMode = true
	c.discoveryRoots = []string{"a/#"}
	c.discoverySubscribed = true
	c.discoveryUntil = time.Now().Add(-time.Second) // lapsed
	c.wildcards["a/#"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true, refCount: 1}

	c.reapDiscovery()
	require.Empty(t, fp.unsubscribed, "discovery must not unsubscribe a filter the wildcard panel still uses")
}

func TestWildcard_ReapKeepsSharedFilterForDiscovery(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.discoveryMode = true
	c.discoveryRoots = []string{"a/#"}
	c.discoverySubscribed = true
	c.discoveryUntil = time.Now().Add(time.Minute) // active
	c.wildcards["a/#"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true, detachedAt: time.Now().Add(-2 * defaultGracePeriod)}

	c.sweepWildcards()
	require.Empty(t, fp.unsubscribed, "wildcard reap must not unsubscribe a filter discovery still uses")
	c.wildMu.RLock()
	_, ok := c.wildcards["a/#"]
	c.wildMu.RUnlock()
	require.False(t, ok, "the wildcard entry is still dropped")
}
