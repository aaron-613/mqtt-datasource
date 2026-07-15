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
}

// detachGracePeriod is how long a topic's ring buffer (and its MQTT subscription) is
// retained after its last consumer detaches, so a re-query — e.g. a zoom, which tears
// down and re-establishes the stream — can reseed recent history instead of blanking.
// janitorInterval is how often abandoned topics are swept.
const (
	detachGracePeriod = 5 * time.Minute
	janitorInterval   = 60 * time.Second
	// maxDiscoveredTopics bounds the discovery registry so a broad wildcard can't grow
	// it without limit.
	maxDiscoveredTopics = 2000
	// discoveryLeaseTTL is how long a StartDiscovery ping keeps the root subscription
	// alive; discoveryReapInterval is how often the janitor checks whether the lease has
	// lapsed. The editor pings well inside the TTL so the lease stays warm while open.
	discoveryLeaseTTL     = 30 * time.Second
	discoveryReapInterval = 10 * time.Second
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

	logger.Info("MQTT Connecting", "clientID", clientID)

	pahoClient := paho.NewClient(opts)
	if token := pahoClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, backend.DownstreamErrorf("error connecting to MQTT broker: %s", token.Error())
	}

	c := &client{
		client:         pahoClient,
		done:           make(chan struct{}),
		discovered:     make(map[string][]byte),
		discoveryMode:  o.DiscoveryMode,
		discoveryRoots: SplitRoots(o.RootTopic),
	}
	go c.janitor()

	return c, nil
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

	// Subscribe outside the lock: paho's token.Wait() can block on the network, and
	// discMu is taken on the recordDiscovered hot path.
	handler := func(_ paho.Client, m paho.Message) {
		c.recordDiscovered(m.Topic(), m.Payload())
	}
	for _, rt := range c.discoveryRoots {
		log.DefaultLogger.Info("MQTT discovery subscribing", "rootTopic", rt)
		if token := c.client.Subscribe(rt, 0, handler); token.Wait() && token.Error() != nil {
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

	// Unsubscribe outside the lock (see StartDiscovery).
	for _, rt := range roots {
		c.client.Unsubscribe(rt)
	}
	log.DefaultLogger.Info("MQTT discovery lapsed", "roots", roots)
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
		case <-discoveryTicker.C:
			c.reapDiscovery()
		}
	}
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
			time.Since(t.stream.detachedAt) > detachGracePeriod
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
			time.Since(t.stream.detachedAt) > detachGracePeriod
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
		return existing
	}
	stored := newStreamTopic(t.Path, t.Interval, t.Fields, t.Window)
	stored.StreamingKey = t.StreamingKey
	stored.FieldAliases = t.FieldAliases
	stored.SeriesName = t.SeriesName
	stored.LabelSource = t.LabelSource
	stored.LabelValue = t.LabelValue
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
	t.stream.mu.Unlock()
	return nil
}

func (c *client) Dispose() {
	log.DefaultLogger.Info("MQTT Disconnecting")
	if c.done != nil {
		close(c.done)
	}
	c.client.Disconnect(250)
}
