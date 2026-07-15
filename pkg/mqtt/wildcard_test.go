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
	top.stream.mu.Lock()
	n := len(top.Messages)
	top.stream.mu.Unlock()
	require.Equal(t, 2, n, "the wildcard subscription feeds the per-series ring buffer")
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

	tp.stream.mu.Lock()
	require.False(t, tp.stream.pahoSubscribed)
	require.Equal(t, "a/+/c", tp.stream.coveredByPattern)
	require.Equal(t, 1, tp.stream.attachedCount)
	tp.stream.mu.Unlock()
	require.Equal(t, 1, c.wildcards["a/+/c"].refCount)

	require.NoError(t, c.Unsubscribe(reqPath, log.DefaultLogger))
	require.Equal(t, 0, c.wildcards["a/+/c"].refCount, "unsubscribe releases the wildcard refcount")
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
	tp.stream.mu.Lock()
	require.Equal(t, "", tp.stream.coveredByPattern)
	require.True(t, tp.stream.pahoSubscribed)
	tp.stream.mu.Unlock()
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

	top.stream.mu.Lock()
	n := len(top.Messages)
	top.stream.mu.Unlock()
	require.Equal(t, 1, n, "no double-append across overlapping patterns")
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

	for _, tp := range []*Topic{t1, t2} {
		tp.stream.mu.Lock()
		n := len(tp.Messages)
		tp.stream.mu.Unlock()
		require.Equal(t, 1, n, "one message fans out once to each panel's buffer")
	}
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
