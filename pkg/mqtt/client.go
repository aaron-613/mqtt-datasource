package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

type Client interface {
	GetTopic(string) (*Topic, bool)
	EnsureTopic(*Topic) *Topic
	IsConnected() bool
	Subscribe(string, log.Logger) (*Topic, error)
	Unsubscribe(string, log.Logger) error
	ListTopics() []string
	SampleFor(string) ([]byte, bool)
	StartDiscovery()
	EnsureWildcard(string) []string
	Dispose()
}

type Options struct {
	URI           string `json:"uri"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	ClientID      string `json:"clientID"`
	TLSCACert     string `json:"tlsCACert"`
	TLSClientCert string `json:"tlsClientCert"`
	TLSClientKey  string `json:"tlsClientKey"`
	TLSSkipVerify bool   `json:"tlsSkipVerify"`
	// Discovery: when enabled, the client subscribes to RootTopic (a wildcard) and
	// records the most recent sample payload per concrete topic, for the topic/field
	// pick-lists. This is independent of graphing subscriptions.
	DiscoveryMode bool   `json:"discoveryMode"`
	RootTopic     string `json:"rootTopic"`
	// MaxSeries caps how many concrete topics a wildcard query fans out into (0 = default).
	MaxSeries int `json:"maxSeries"`
	// RestrictTopics, when enabled, scopes queries to the configured root(s): a topic
	// outside every root is soft-rejected (a notice, not a subscription). A tidiness
	// guardrail, not security.
	RestrictTopics bool `json:"restrictTopics"`
	// GraceSeconds is how long a subscription + recent-history buffer is retained after a
	// panel detaches (so a zoom/pan/refresh reseeds instead of blanking). 0 = default (30s).
	GraceSeconds int `json:"graceSeconds"`
}

// defaultGracePeriod is how long a topic's ring buffer (and its MQTT subscription) is retained
// after its last consumer detaches, so a re-query — e.g. a zoom/pan/refresh, which briefly tears
// down and re-establishes the stream — reseeds recent history instead of blanking. It is the
// fallback when the datasource doesn't configure its own grace (Options.GraceSeconds).
// janitorInterval is how often abandoned topics are swept; keep it at/below the grace so a short
// grace actually reaps promptly.
const (
	defaultGracePeriod = 30 * time.Second
	janitorInterval    = 15 * time.Second
	// maxDiscoveredTopics bounds the discovery registry so a broad wildcard can't grow
	// it without limit.
	maxDiscoveredTopics = 2000
	// discoveryLeaseTTL is how long a StartDiscovery ping keeps the root subscription
	// alive; discoveryReapInterval is how often the janitor checks whether the lease has
	// lapsed. The editor pings well inside the TTL so the lease stays warm while open.
	discoveryLeaseTTL     = 30 * time.Second
	discoveryReapInterval = 10 * time.Second
	// maxWildcardSeen caps each pattern's seen-set (reusing the discovery cap).
	maxWildcardSeen = maxDiscoveredTopics
)

// wildcardColdStartWait bounds how long EnsureWildcard blocks on first use, waiting for the
// panel's own wildcard subscription to observe its first concrete topics (retained brokers
// return instantly; a live non-retained broker fills within ~one publish cadence).
// wildcardPollInterval is how often the wait re-checks. Vars (not consts) so tests can shorten
// them.
var (
	wildcardColdStartWait = 1200 * time.Millisecond
	wildcardPollInterval  = 50 * time.Millisecond
)

type client struct {
	client paho.Client
	topics TopicMap
	done   chan struct{}

	// discovered holds the most recent sample payload per concrete topic seen on the
	// discovery (root wildcard) subscription. Used only for the pick-lists. discOrder
	// tracks first-seen insertion order for FIFO eviction once the cap is reached, so a
	// full registry admits newly-appearing topics (dropping the oldest) rather than
	// dropping the new ones.
	discMu     sync.RWMutex
	discovered map[string][]byte
	discOrder  []string

	// Discovery lifecycle: the root subscription runs on demand — StartDiscovery (driven
	// by the editor's keep-alive pings) subscribes and bumps the lease; reapDiscovery
	// unsubscribes once the lease lapses. discoveryUntil and discoverySubscribed are
	// guarded by discMu. The discovered cache survives a lapse so pick-lists still render.
	discoveryMode       bool
	discoveryRoots      []string
	discoveryUntil      time.Time
	discoverySubscribed bool

	// gracePeriod is the retain-after-detach window (from Options.GraceSeconds, else default).
	gracePeriod time.Duration

	// Wildcard graphing: a wildcard query opens ONE paho subscription to its OWN pattern
	// (narrower than the discovery root) and demuxes each message by concrete topic into the
	// per-series ring buffers — instead of one subscription per matched concrete topic. All
	// wildcard/discovery subscriptions use a nil paho callback and route through the single
	// default publish handler (dispatch), so overlapping or identical filter strings never
	// clobber one another or double-deliver. wildcards is keyed by the MQTT filter string and
	// guarded by wildMu.
	wildMu    sync.RWMutex
	wildcards map[string]*wildcardSub
}

// wildcardSub tracks one active wildcard subscription: the concrete topics it has observed
// (for query-time enumeration, FIFO-capped), whether its paho subscription is open, and a
// reference count of the per-series channels currently attached under it (with detachedAt for
// janitor reaping once idle past the grace period).
type wildcardSub struct {
	seen           map[string]struct{}
	seenOrder      []string
	pahoSubscribed bool
	refCount       int
	detachedAt     time.Time
}

// SplitRoots parses a comma-separated list of root topic filters into trimmed,
// non-empty entries.
func SplitRoots(s string) []string {
	var roots []string
	for _, rt := range strings.Split(s, ",") {
		if rt = strings.TrimSpace(rt); rt != "" {
			roots = append(roots, rt)
		}
	}
	return roots
}

func NewClient(ctx context.Context, o Options, settings backend.DataSourceInstanceSettings) (Client, error) {
	logger := log.DefaultLogger.FromContext(ctx)
	opts := paho.NewClientOptions()

	opts.AddBroker(o.URI)

	clientID := o.ClientID
	if clientID == "" {
		// A random client ID keeps each connection unique — including across active-active HA
		// Grafana nodes, where a stable per-datasource ID would collide and the nodes would
		// take the connection from one another. With the clean session below it never
		// accumulates broker-side. (Set a fixed Client ID in the datasource config to override.)
		clientID = fmt.Sprintf("grafana_%d", rand.Int())
	}
	opts.SetClientID(clientID)

	if o.Username != "" {
		opts.SetUsername(o.Username)
	}

	if o.Password != "" {
		opts.SetPassword(o.Password)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: o.TLSSkipVerify,
	}

	if o.TLSClientCert != "" || o.TLSClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(o.TLSClientCert), []byte(o.TLSClientKey))
		if err != nil {
			return nil, backend.DownstreamErrorf("failed to setup TLSClientCert: %w", err)
		}

		tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	}

	if o.TLSCACert != "" {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(o.TLSCACert))
		tlsConfig.RootCAs = caCertPool
	}

	opts.SetTLSConfig(tlsConfig)
	opts.SetPingTimeout(60 * time.Second)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetAutoReconnect(true)
	// Use a clean session. All subscriptions are QoS 0 (at most once), so a persistent
	// session would queue nothing for redelivery — its only effect here would be durable
	// session state accumulating on the broker for every (randomly-named) client that ever
	// connected, which never gets resumed or cleaned up. paho re-subscribes on reconnect.
	opts.SetCleanSession(true)
	opts.SetMaxReconnectInterval(10 * time.Second)
	opts.SetConnectionLostHandler(func(c paho.Client, err error) {
		logger.Warn("MQTT Connection lost", "error", err)
	})
	opts.SetReconnectingHandler(func(c paho.Client, options *paho.ClientOptions) {
		logger.Debug("MQTT Reconnecting")
	})

	// Configure PDC (Private Datasource Connect) if enabled
	if err := configureProxyIfEnabled(ctx, opts, settings, logger); err != nil {
		return nil, err
	}

	gracePeriod := time.Duration(o.GraceSeconds) * time.Second
	if gracePeriod <= 0 {
		gracePeriod = defaultGracePeriod
	}
	c := &client{
		done:           make(chan struct{}),
		discovered:     make(map[string][]byte),
		wildcards:      make(map[string]*wildcardSub),
		discoveryMode:  o.DiscoveryMode,
		discoveryRoots: SplitRoots(o.RootTopic),
		gracePeriod:    gracePeriod,
	}
	// Wildcard and discovery subscriptions are made with a nil callback, so their messages
	// route to this single default handler, which demuxes by concrete topic. This is what
	// makes overlapping/identical filter strings safe (paho keeps no per-filter route to
	// clobber, and the default handler fires exactly once per message).
	opts.SetDefaultPublishHandler(c.dispatch)

	logger.Info("MQTT Connecting", "clientID", clientID)

	pahoClient := paho.NewClient(opts)
	if token := pahoClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, backend.DownstreamErrorf("error connecting to MQTT broker: %s", token.Error())
	}
	c.client = pahoClient
	go c.janitor()

	return c, nil
}

// dispatch is the single default publish handler for all nil-callback subscriptions
// (discovery roots and wildcard patterns). It fires exactly once per message regardless of
// how many subscriptions match, so it can safely demux without double-delivery: it updates
// the discovery registry, records the concrete topic into every matching wildcard seen-set,
// and — only when a wildcard pattern matches — feeds the per-series ring buffers.
func (c *client) dispatch(_ paho.Client, m paho.Message) {
	topic := m.Topic()
	payload := m.Payload()
	if c.discoveryMode {
		c.recordDiscovered(topic, payload)
	}
	if c.recordWildcardSeen(topic) {
		c.HandleMessage(encodeTopic(topic), payload)
	}
}

// StartDiscovery is the keep-alive for on-demand discovery: the query editor's resource
// calls (/topics, /fields) invoke it while an editor is open. It bumps the lease and, on
// the first call (or after a lapse), subscribes to each root wildcard so the discovery
// registry fills. It is a no-op when discovery is disabled or no roots are configured.
func (c *client) StartDiscovery() {
	if !c.discoveryMode || len(c.discoveryRoots) == 0 {
		return
	}

	c.discMu.Lock()
	c.discoveryUntil = time.Now().Add(discoveryLeaseTTL)
	needSubscribe := !c.discoverySubscribed
	c.discoverySubscribed = true
	c.discMu.Unlock()

	if !needSubscribe {
		return
	}

	// Subscribe outside the lock: paho's token.Wait() can block on the network. A nil callback
	// routes root messages to the default handler (dispatch), which does recordDiscovered.
	for _, rt := range c.discoveryRoots {
		log.DefaultLogger.Info("MQTT discovery subscribing", "rootTopic", rt)
		if token := c.client.Subscribe(rt, 0, nil); token.Wait() && token.Error() != nil {
			// Discovery is best-effort; a bad root topic shouldn't fail the datasource.
			log.DefaultLogger.Warn("MQTT discovery subscribe failed", "rootTopic", rt, "error", token.Error())
		}
	}
}

// reapDiscovery unsubscribes from the root wildcards once the keep-alive lease has
// lapsed, stopping the firehose. The discovered cache is retained so pick-lists still
// render (stale) until the next StartDiscovery tops it up.
func (c *client) reapDiscovery() {
	c.discMu.Lock()
	lapsed := c.discoverySubscribed && time.Now().After(c.discoveryUntil)
	if lapsed {
		c.discoverySubscribed = false
	}
	roots := c.discoveryRoots
	c.discMu.Unlock()

	if !lapsed {
		return
	}

	// Unsubscribe outside the lock (see StartDiscovery). Skip a root that an active wildcard
	// panel is still using as its exact filter string — the broker keeps a single
	// subscription per filter, so unsubscribing it would cut off the wildcard panel too.
	for _, rt := range roots {
		if c.wildcardActive(rt) {
			continue
		}
		c.client.Unsubscribe(rt)
	}
	log.DefaultLogger.Info("MQTT discovery lapsed", "roots", roots)
}

// wildcardActive reports whether a filter string is currently an open wildcard subscription.
func (c *client) wildcardActive(filter string) bool {
	c.wildMu.RLock()
	defer c.wildMu.RUnlock()
	ws, ok := c.wildcards[filter]
	return ok && ws.pahoSubscribed
}

// discoveryActiveRoot reports whether a filter string is currently an active discovery root.
func (c *client) discoveryActiveRoot(filter string) bool {
	c.discMu.RLock()
	defer c.discMu.RUnlock()
	if !c.discoverySubscribed {
		return false
	}
	for _, rt := range c.discoveryRoots {
		if rt == filter {
			return true
		}
	}
	return false
}

// recordDiscovered stores the latest sample payload for a concrete topic seen on the
// discovery subscription, bounded by maxDiscoveredTopics.
func (c *client) recordDiscovered(topic string, payload []byte) {
	c.discMu.Lock()
	defer c.discMu.Unlock()
	sample := make([]byte, len(payload))
	copy(sample, payload)

	if _, exists := c.discovered[topic]; !exists {
		if len(c.discovered) >= maxDiscoveredTopics && len(c.discOrder) > 0 {
			// Evict the oldest first-seen topic so a newly-appearing one is admitted.
			oldest := c.discOrder[0]
			c.discOrder = c.discOrder[1:]
			delete(c.discovered, oldest)
		}
		c.discOrder = append(c.discOrder, topic)
	}
	c.discovered[topic] = sample
}

// ListTopics returns the discovered topics, sorted.
func (c *client) ListTopics() []string {
	c.discMu.RLock()
	defer c.discMu.RUnlock()
	topics := make([]string, 0, len(c.discovered))
	for t := range c.discovered {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	return topics
}

// SampleFor returns the most recent sample payload for a discovered topic.
func (c *client) SampleFor(topic string) ([]byte, bool) {
	c.discMu.RLock()
	defer c.discMu.RUnlock()
	sample, ok := c.discovered[topic]
	return sample, ok
}

// EnsureWildcard opens (idempotently) ONE paho subscription for a wildcard panel's own
// pattern and returns the concrete topics observed under it so far — the enumeration source
// for queryWildcard, replacing the discovery registry. The subscription's messages route to
// dispatch (nil callback), which feeds the per-series buffers. On a cold first call it waits
// briefly for the first topics to arrive (retained brokers return instantly); an empty result
// lets the caller show the "warming up" notice and fill on the next refresh.
func (c *client) EnsureWildcard(pattern string) []string {
	if pattern == "" {
		return nil
	}

	c.wildMu.Lock()
	ws := c.wildcards[pattern]
	if ws == nil {
		// Born idle (detachedAt=now) so an orphan sub — opened here but whose channels never
		// attach — is still reapable by the janitor after the grace period.
		ws = &wildcardSub{seen: make(map[string]struct{}), detachedAt: time.Now()}
		c.wildcards[pattern] = ws
	}
	needSubscribe := !ws.pahoSubscribed
	ws.pahoSubscribed = true
	if ws.refCount == 0 {
		ws.detachedAt = time.Now() // refresh grace on a re-query while still idle
	}
	empty := len(ws.seen) == 0
	c.wildMu.Unlock()

	if needSubscribe {
		// nil callback → messages route to dispatch; see the comment in NewClient.
		if token := c.client.Subscribe(pattern, 0, nil); token.Wait() && token.Error() != nil {
			c.wildMu.Lock()
			ws.pahoSubscribed = false
			c.wildMu.Unlock()
			log.DefaultLogger.Warn("MQTT wildcard subscribe failed", "pattern", pattern, "error", token.Error())
			return nil
		}
		log.DefaultLogger.Info("MQTT wildcard subscribing", "pattern", pattern)
	}

	if empty {
		c.awaitSeen(pattern)
	}
	return c.wildcardSeen(pattern)
}

// awaitSeen blocks up to wildcardColdStartWait for a pattern's seen-set to become non-empty,
// polling cheaply. Aborts immediately on Dispose.
func (c *client) awaitSeen(pattern string) {
	deadline := time.Now().Add(wildcardColdStartWait)
	ticker := time.NewTicker(wildcardPollInterval)
	defer ticker.Stop()
	for {
		if len(c.wildcardSeen(pattern)) > 0 {
			return
		}
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if time.Now().After(deadline) {
				return
			}
		}
	}
}

// wildcardSeen returns the sorted concrete topics observed under a pattern.
func (c *client) wildcardSeen(pattern string) []string {
	c.wildMu.RLock()
	defer c.wildMu.RUnlock()
	ws := c.wildcards[pattern]
	if ws == nil {
		return nil
	}
	out := make([]string, 0, len(ws.seen))
	for t := range ws.seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// recordWildcardSeen records a concrete topic into every active wildcard pattern that matches
// it (FIFO-capped per pattern) and reports whether ANY matched — i.e. whether the message
// should feed the ring buffers. Called from dispatch with the topic decoded.
func (c *client) recordWildcardSeen(topic string) bool {
	c.wildMu.Lock()
	defer c.wildMu.Unlock()
	matched := false
	for pat, ws := range c.wildcards {
		if !ws.pahoSubscribed {
			continue
		}
		if _, ok := MatchTopic(pat, topic); !ok {
			continue
		}
		matched = true
		if _, seen := ws.seen[topic]; !seen {
			if len(ws.seen) >= maxWildcardSeen && len(ws.seenOrder) > 0 {
				oldest := ws.seenOrder[0]
				ws.seenOrder = ws.seenOrder[1:]
				delete(ws.seen, oldest)
			}
			ws.seen[topic] = struct{}{}
			ws.seenOrder = append(ws.seenOrder, topic)
		}
	}
	return matched
}

// attachWildcardExact attaches a series to the specific wildcard subscription that ENUMERATED
// it (bumping the refcount so it stays alive while the series streams), if that subscription is
// active. Coverage is deliberately scoped to the enumerating pattern — a series never rides some
// unrelated broad subscription that merely happens to match it — so panels stay independent and
// broker topology is order-independent. Increment and the reap decision (sweepWildcards) are
// both under wildMu, closing the reap-vs-reattach race.
func (c *client) attachWildcardExact(pattern string) bool {
	c.wildMu.Lock()
	defer c.wildMu.Unlock()
	if ws, ok := c.wildcards[pattern]; ok && ws.pahoSubscribed {
		ws.refCount++
		ws.detachedAt = time.Time{}
		return true
	}
	return false
}

// detachWildcard releases one covered-channel reference; when the last one detaches the sub
// becomes eligible for reaping after the grace period.
func (c *client) detachWildcard(pattern string) {
	c.wildMu.Lock()
	defer c.wildMu.Unlock()
	if ws := c.wildcards[pattern]; ws != nil && ws.refCount > 0 {
		ws.refCount--
		if ws.refCount == 0 {
			ws.detachedAt = time.Now()
		}
	}
}

// sweepWildcards unsubscribes and drops wildcard subs with no attached channels past the grace
// period (two-phase re-check, like sweep()). It skips a filter still needed as an active
// discovery root, and does paho Unsubscribe outside the lock.
func (c *client) sweepWildcards() {
	var candidates []string
	c.wildMu.RLock()
	for pat, ws := range c.wildcards {
		if ws.refCount == 0 && !ws.detachedAt.IsZero() && time.Since(ws.detachedAt) > c.grace() {
			candidates = append(candidates, pat)
		}
	}
	c.wildMu.RUnlock()

	for _, pat := range candidates {
		c.wildMu.Lock()
		ws, ok := c.wildcards[pat]
		stale := ok && ws.refCount == 0 && !ws.detachedAt.IsZero() && time.Since(ws.detachedAt) > c.grace()
		if stale {
			ws.pahoSubscribed = false
			delete(c.wildcards, pat)
		}
		c.wildMu.Unlock()
		if !stale {
			continue
		}
		if !c.discoveryActiveRoot(pat) {
			c.client.Unsubscribe(pat)
		}
		log.DefaultLogger.Info("MQTT wildcard subscription reaped", "pattern", pat)
	}
}

// janitor periodically removes topics whose consumers have all detached for longer
// than the grace period, closing their MQTT subscription and freeing the ring buffer.
func (c *client) janitor() {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()
	discoveryTicker := time.NewTicker(discoveryReapInterval)
	defer discoveryTicker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.sweep()
			c.sweepWildcards()
		case <-discoveryTicker.C:
			c.reapDiscovery()
		}
	}
}

// grace returns the configured retain-after-detach window (default if unset).
func (c *client) grace() time.Duration {
	if c.gracePeriod > 0 {
		return c.gracePeriod
	}
	return defaultGracePeriod
}

func (c *client) sweep() {
	type victim struct {
		key    string
		mqtt   string
		hadSub bool
	}
	var victims []victim
	c.topics.Range(func(key, v any) bool {
		t, ok := v.(*Topic)
		if !ok || t.stream == nil {
			return true
		}
		t.stream.mu.Lock()
		stale := t.stream.attachedCount == 0 && !t.stream.detachedAt.IsZero() &&
			time.Since(t.stream.detachedAt) > c.grace()
		hadSub := t.stream.pahoSubscribed
		t.stream.mu.Unlock()
		if stale {
			mqttTopic, err := decodeTopic(t.Path, log.DefaultLogger)
			if err == nil {
				victims = append(victims, victim{key: key.(string), mqtt: mqttTopic, hadSub: hadSub})
			}
		}
		return true
	})

	for _, vv := range victims {
		// Re-check under lock so a topic that re-attached since the scan is spared.
		t, ok := c.topics.Load(vv.key)
		if !ok || t.stream == nil {
			continue
		}
		t.stream.mu.Lock()
		stillStale := t.stream.attachedCount == 0 && !t.stream.detachedAt.IsZero() &&
			time.Since(t.stream.detachedAt) > c.grace()
		if stillStale {
			t.stream.pahoSubscribed = false
		}
		t.stream.mu.Unlock()
		if !stillStale {
			continue
		}
		if vv.hadSub {
			c.client.Unsubscribe(vv.mqtt)
		}
		c.topics.Delete(vv.key)
		log.DefaultLogger.Debug("janitor removed idle topic", "key", vv.key)
	}
}

func (c *client) IsConnected() bool {
	return c.client.IsConnectionOpen()
}

func (c *client) HandleMessage(topic string, payload []byte) {
	message := Message{
		Timestamp: time.Now(),
		Value:     payload,
	}

	c.topics.AddMessage(topic, message)
}

func (c *client) GetTopic(reqPath string) (*Topic, bool) {
	return c.topics.Load(reqPath)
}

// EnsureTopic registers a topic (with its field selection and buffer) if one does
// not already exist for its key, without opening an MQTT subscription. QueryData
// calls this so the ring buffer and framer are configured before streaming starts;
// the returned topic is the live instance stored in the map.
func (c *client) EnsureTopic(t *Topic) *Topic {
	if existing, ok := c.topics.Load(t.Key()); ok {
		// A topic may already exist without the right field selection (e.g. created by
		// the RunStream fallback on a Live reconnect before QueryData ran). Reconcile it.
		existing.reconcileFields(t.Fields, t.FieldAliases, t.SeriesName, t.LabelSource, t.LabelValue)
		existing.stream.mu.Lock()
		existing.stream.enumeratedBy = t.EnumeratedBy
		existing.stream.mu.Unlock()
		return existing
	}
	stored := newStreamTopic(t.Path, t.Interval, t.Fields, t.Window)
	stored.StreamingKey = t.StreamingKey
	stored.FieldAliases = t.FieldAliases
	stored.SeriesName = t.SeriesName
	stored.LabelSource = t.LabelSource
	stored.LabelValue = t.LabelValue
	// The wildcard pattern (if any) that enumerated this series, so Subscribe attaches it to
	// that exact pattern's shared subscription rather than opening a per-topic one.
	stored.stream.enumeratedBy = t.EnumeratedBy
	// Seed the raw buffer from an existing sibling on the same MQTT topic so a new field
	// selection (its streaming key changes when fields change) opens with recent history
	// instead of blanking and restarting.
	if seed := c.siblingBuffer(t.Path); len(seed) > 0 {
		stored.Messages = seed
	}
	c.topics.Map.Store(t.Key(), stored)
	return stored
}

// siblingBuffer returns a copy of the largest ring buffer among already-registered
// topics sharing the same MQTT path (base64), used to seed a newly-created topic.
func (c *client) siblingBuffer(path string) []Message {
	var best []Message
	c.topics.Range(func(_, v any) bool {
		topic, ok := v.(*Topic)
		if !ok || topic.Path != path || topic.stream == nil {
			return true
		}
		topic.stream.mu.Lock()
		if len(topic.Messages) > len(best) {
			best = append([]Message(nil), topic.Messages...)
		}
		topic.stream.mu.Unlock()
		return true
	})
	return best
}

func (c *client) Subscribe(reqPath string, logger log.Logger) (*Topic, error) {
	chunks := strings.Split(reqPath, "/")
	if len(chunks) < 2 {
		return nil, backend.DownstreamErrorf("invalid path: %s", reqPath)
	}
	interval, err := time.ParseDuration(chunks[0])
	if err != nil {
		return nil, backend.DownstreamErrorf("invalid interval %s: %s", chunks[0], err)
	}

	// Find an already-registered topic (usually created by QueryData via EnsureTopic),
	// or create a default (classic, no field selection) one.
	t, ok := c.topics.Load(reqPath)
	if !ok {
		// Path is ONLY the base64 topic segment (not the streaming-key suffix). Every
		// topic subscribed to the same MQTT topic must share the same Path so that an
		// incoming message fans out to all of them via TopicMap.AddMessage — otherwise,
		// because paho keeps a single handler per topic filter, two panels on the same
		// topic would starve one another.
		//
		// StreamingKey must also be set from the remaining segments so the topic's Key()
		// equals reqPath and QueryData rebuilds a well-formed channel for it (otherwise
		// SubscribeStream rejects the channel as "invalid channel path format").
		topicPath := chunks[1]
		t = newStreamTopic(topicPath, interval, nil, 0)
		t.StreamingKey = strings.Join(chunks[2:], "/")
		c.topics.Map.Store(reqPath, t)
	}

	// Coverage: if this series was enumerated by a wildcard panel whose subscription is active,
	// attach to THAT pattern's shared subscription WITHOUT opening a per-topic paho subscription
	// (it feeds this topic's buffer via dispatch). Coverage is scoped to the enumerating pattern
	// only, so an independent concrete panel is never absorbed into some unrelated broad sub.
	// pahoSubscribed stays false so the janitor won't paho-Unsubscribe a topic that was never
	// individually subscribed.
	t.stream.mu.Lock()
	hint := t.stream.enumeratedBy
	t.stream.mu.Unlock()
	if hint != "" && c.attachWildcardExact(hint) {
		t.stream.mu.Lock()
		t.stream.attachedCount++
		t.stream.detachedAt = time.Time{}
		t.stream.coveredByPattern = hint
		t.stream.mu.Unlock()
		return t, nil
	}

	// Register this consumer and open the MQTT subscription once per topic. The
	// subscription and buffer are kept alive across detach so zoom re-queries reseed.
	t.stream.mu.Lock()
	t.stream.attachedCount++
	t.stream.detachedAt = time.Time{}
	needSubscribe := !t.stream.pahoSubscribed
	t.stream.pahoSubscribed = true
	t.stream.mu.Unlock()
	if !needSubscribe {
		return t, nil
	}

	revert := func() {
		t.stream.mu.Lock()
		t.stream.pahoSubscribed = false
		if t.stream.attachedCount > 0 {
			t.stream.attachedCount--
		}
		t.stream.mu.Unlock()
	}

	topic, err := decodeTopic(t.Path, logger)
	if err != nil {
		revert()
		return nil, backend.DownstreamErrorf("error decoding MQTT topic name %s: %s", t.Path, err)
	}

	logger.Debug("Subscribing to MQTT topic", "topic", topic)

	routePath := t.Path
	if token := c.client.Subscribe(topic, 0, func(_ paho.Client, m paho.Message) {
		// by wrapping HandleMessage we get the correct topicPath for the incoming topic
		// and don't need to regex it against + and #.
		c.HandleMessage(routePath, []byte(m.Payload()))
	}); token.Wait() && token.Error() != nil {
		revert()
		return nil, backend.DownstreamErrorf("error subscribing to MQTT topic %s: %s", topic, token.Error())
	}
	return t, nil
}

func (c *client) Unsubscribe(reqPath string, _ log.Logger) error {
	t, ok := c.GetTopic(reqPath)
	if !ok {
		return nil // No error if topic doesn't exist
	}

	// Detach this consumer but RETAIN the topic, its ring buffer, and the MQTT
	// subscription so a re-query (e.g. a zoom, which tears down and re-establishes the
	// stream) can reseed recent history instead of blanking. The janitor closes the
	// subscription and frees the buffer once the topic stays detached past the grace
	// period.
	t.stream.mu.Lock()
	if t.stream.attachedCount > 0 {
		t.stream.attachedCount--
	}
	if t.stream.attachedCount == 0 {
		t.stream.detachedAt = time.Now()
	}
	pattern := t.stream.coveredByPattern
	if t.stream.attachedCount == 0 {
		t.stream.coveredByPattern = ""
	}
	t.stream.mu.Unlock()
	// Release this channel's reference on the covering wildcard sub (if any) so it can be
	// reaped once all its covered channels are gone.
	if pattern != "" {
		c.detachWildcard(pattern)
	}
	return nil
}

func (c *client) Dispose() {
	log.DefaultLogger.Info("MQTT Disconnecting")
	if c.done != nil {
		close(c.done)
	}
	c.client.Disconnect(250)
}
