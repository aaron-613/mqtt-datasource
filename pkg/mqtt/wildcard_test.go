package mqtt

import (
	"sync"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/stretchr/testify/require"
)

// TestSweep_SparesRawReattachedDuringSweep deterministically exercises the F-1 fix: a consumer
// re-subscribes AFTER the janitor has selected the idle raw as a reap victim but BEFORE the
// guarded delete. The phase-2 re-check (under rawMu+r.mu) must see the bumped refcount and spare
// the raw — not delete it or unsubscribe it. Fails if the two-phase re-check is ever removed.
func TestSweep_SparesRawReattachedDuringSweep(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.gracePeriod = time.Millisecond

	path := encodeTopic("x/y/z")
	reqPath := "1s/" + path + "/k"

	// Prime an idle, reap-eligible raw (refCount 0, aged past grace).
	c.EnsureTopic(&Topic{Path: path, Interval: time.Second, StreamingKey: "k"})
	_, err := c.Subscribe(reqPath, log.DefaultLogger)
	require.NoError(t, err)
	require.NoError(t, c.Unsubscribe(reqPath, log.DefaultLogger))
	time.Sleep(2 * time.Millisecond)

	// Re-subscribe in the gap between victim selection and the guarded delete.
	c.sweepAfterScan = func() {
		c.sweepAfterScan = nil // fire once
		c.EnsureTopic(&Topic{Path: path, Interval: time.Second, StreamingKey: "k"})
		if _, err := c.Subscribe(reqPath, log.DefaultLogger); err != nil {
			t.Error(err)
		}
	}
	c.sweep()

	// The re-attached raw must be spared and fully functional.
	c.rawMu.RLock()
	r, ok := c.raws[path]
	c.rawMu.RUnlock()
	require.True(t, ok, "re-attached raw must not be reaped mid-sweep")
	require.Equal(t, 1, r.refCount)
	require.Empty(t, fp.unsubscribed, "a spared raw must not be unsubscribed")
	c.HandleMessage(path, []byte(`{"v":1}`))
	top, ok := c.GetTopic(reqPath)
	require.True(t, ok, "the view must survive")
	require.Equal(t, 1, top.bufferLen(), "spared topic is still fed")
}

// TestSweep_RaceWithReSubscribe is the concurrency-safety guard for the attach/detach/feed/sweep
// paths: ONE consumer (QueryData→RunStream is serial per channel) churns attach→feed→detach while
// several janitors sweep. Run with -race — it caught the unlocked stream.raw reads during
// development. The F-1 TOCTOU itself is fixed structurally (attach and reap serialize on rawMu, so
// the vulnerable interleaving no longer exists); the deterministic re-check guard is above.
func TestSweep_RaceWithReSubscribe(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	c.gracePeriod = time.Millisecond // idle raws are reap-eligible almost immediately

	path := encodeTopic("x/y/z")
	reqPath := "1s/" + path + "/k"

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Several janitors sweep continuously to widen the reap-vs-attach window.
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c.sweep()
				}
			}
		}()
	}

	// One consumer: attach → feed → assert fed → detach, repeatedly (RunStream lifecycle churn).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20000; i++ {
			c.EnsureTopic(&Topic{Path: path, Interval: time.Second, StreamingKey: "k"})
			if _, err := c.Subscribe(reqPath, log.DefaultLogger); err != nil {
				t.Error(err)
				return
			}
			// While attached (refCount>=1) the raw must be live and fed — never reaped out from under us.
			c.HandleMessage(path, []byte(`{"v":1}`))
			if top, ok := c.GetTopic(reqPath); ok && top.bufferLen() == 0 {
				t.Error("attached topic was orphaned: shared buffer not fed")
				return
			}
			_ = c.Unsubscribe(reqPath, log.DefaultLogger)
		}
	}()

	<-done
	close(stop)
	wg.Wait()
}

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
	require.False(t, raw.pahoSubscribed, "a wildcard-covered topic has no own subscription")
	require.Equal(t, 1, raw.refCount)
	raw.mu.Unlock()
	tp.stream.mu.Lock()
	require.Equal(t, "a/+/c", tp.stream.wildcardRef, "the view rides the enumerating pattern's sub")
	tp.stream.mu.Unlock()
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

// TestSubscribe_ConcreteFeedsOverlappingWildcardSeenSet guards the overlap behavior: a concrete
// panel on a topic that a wildcard panel also covers. Since all subscriptions are nil-callback and
// route through the single dispatch handler (no explicit route to shadow it), a delivered message
// is recorded into the overlapping wildcard's seen-set AND feeds the shared buffer — so the
// wildcard panel enumerates the topic even though a concrete panel subscribes to it too.
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
	require.Empty(t, fp.routes, "concrete subs use a nil callback — no explicit route to shadow dispatch")

	// A broker message routes through the single default handler.
	c.deliver("a/b", []byte(`{"v":1}`))

	require.Equal(t, []string{"a/b"}, c.wildcardSeen("a/+"),
		"dispatch records the concrete topic into an overlapping wildcard's seen-set")
	tp, ok := c.GetTopic(reqPath)
	require.True(t, ok)
	require.Equal(t, 1, tp.bufferLen(), "the shared buffer is fed for the concrete view")
}

// TestSubscribe_ConcreteNotAbsorbedByOverlappingWildcard: a concrete panel on a topic a wildcard
// also covers always opens its OWN subscription (never absorbed into the wildcard firehose),
// regardless of attach order. The wildcard-enumerated view independently rides its pattern's
// subscription; the two feeds coexist (dispatch appends once per delivered message). There is no
// order-dependent "upgrade" — a concrete view and a wildcard view simply keep their own feeds.
func TestSubscribe_ConcreteNotAbsorbedByOverlappingWildcard(t *testing.T) {
	concrete := "1s/" + encodeTopic("a/b") + "/kc"
	wild := "1s/" + encodeTopic("a/b") + "/kw"

	assertCoexist := func(t *testing.T, c *client, fp *fakePaho) {
		raw := c.raws[encodeTopic("a/b")]
		require.True(t, raw.pahoSubscribed, "concrete panel opens its own subscription")
		require.Equal(t, []string{"a/b"}, fp.subscribed, "exactly one concrete broker sub for the topic")
		require.Equal(t, 1, c.wildcards["a/+"].refCount, "the wildcard view independently holds its pattern ref")
	}

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

		assertCoexist(t, c, fp)
	})

	t.Run("wildcard first", func(t *testing.T) {
		fp := &fakePaho{}
		c := newWildcardTestClient(fp)
		c.wildcards["a/+"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

		c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), Interval: time.Second, StreamingKey: "kw", EnumeratedBy: "a/+"})
		_, err := c.Subscribe(wild, log.DefaultLogger)
		require.NoError(t, err)
		raw := c.raws[encodeTopic("a/b")]
		require.False(t, raw.pahoSubscribed, "wildcard-only topic: no own subscription")
		require.Empty(t, fp.subscribed, "wildcard-only topic opens no concrete sub")
		require.Equal(t, 1, c.wildcards["a/+"].refCount)

		// A concrete panel arrives: it opens its own sub; the wildcard view keeps its ref. No upgrade.
		c.EnsureTopic(&Topic{Path: encodeTopic("a/b"), Interval: time.Second, StreamingKey: "kc"})
		_, err = c.Subscribe(concrete, log.DefaultLogger)
		require.NoError(t, err)

		assertCoexist(t, c, fp)
	})
}

// TestFifoInsert covers the shared FIFO-capped eviction used by both registries: oldest-out at
// capacity, in-place update (no reorder/growth) for an existing key, and the "added" flag.
func TestFifoInsert(t *testing.T) {
	m := map[string]struct{}{}
	var order []string
	for _, tp := range []string{"a", "b", "c"} { // capacity 2 -> "a" evicted
		var added bool
		order, added = fifoInsert(m, order, tp, struct{}{}, 2)
		require.True(t, added)
	}
	require.Equal(t, []string{"b", "c"}, order)
	require.Len(t, m, 2)
	_, hasA := m["a"]
	require.False(t, hasA, "oldest first-seen topic is evicted at capacity")

	// Re-inserting an existing key updates in place: not "added", no reorder, no growth.
	var added bool
	order, added = fifoInsert(m, order, "b", struct{}{}, 2)
	require.False(t, added)
	require.Equal(t, []string{"b", "c"}, order)

	// Values are updated for an existing key.
	vm := map[string]int{}
	var vo []string
	vo, _ = fifoInsert(vm, vo, "x", 1, 5)
	_, added = fifoInsert(vm, vo, "x", 2, 5)
	require.False(t, added)
	require.Equal(t, 2, vm["x"])
}

// TestRecordWildcardSeen_FastPath covers the read-lock fast path + slow-path insert.
func TestRecordWildcardSeen_FastPath(t *testing.T) {
	c := newWildcardTestClient(&fakePaho{})

	// No wildcard subscriptions -> not matched, nothing recorded.
	require.False(t, c.recordWildcardSeen("a/b"))

	c.wildcards["a/+"] = &wildcardSub{seen: map[string]struct{}{}, pahoSubscribed: true}

	// Matching, new topic -> matched + recorded (slow path).
	require.True(t, c.recordWildcardSeen("a/b"))
	require.Equal(t, []string{"a/b"}, c.wildcardSeen("a/+"))

	// Matching, already seen -> matched, no duplicate (fast path, read-lock only).
	require.True(t, c.recordWildcardSeen("a/b"))
	require.Equal(t, []string{"a/b"}, c.wildcardSeen("a/+"))

	// Non-matching topic -> not matched.
	require.False(t, c.recordWildcardSeen("x/y"))

	// An unsubscribed pattern does not match.
	c.wildcards["a/+"].pahoSubscribed = false
	require.False(t, c.recordWildcardSeen("a/c"))
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
