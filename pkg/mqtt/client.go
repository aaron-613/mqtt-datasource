package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"strings"
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
}

// detachGracePeriod is how long a topic's ring buffer (and its MQTT subscription) is
// retained after its last consumer detaches, so a re-query — e.g. a zoom, which tears
// down and re-establishes the stream — can reseed recent history instead of blanking.
// janitorInterval is how often abandoned topics are swept.
const (
	detachGracePeriod = 5 * time.Minute
	janitorInterval   = 60 * time.Second
)

type client struct {
	client paho.Client
	topics TopicMap
	done   chan struct{}
}

func NewClient(ctx context.Context, o Options, settings backend.DataSourceInstanceSettings) (Client, error) {
	logger := log.DefaultLogger.FromContext(ctx)
	opts := paho.NewClientOptions()

	opts.AddBroker(o.URI)

	clientID := o.ClientID
	if clientID == "" {
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
	opts.SetCleanSession(false)
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
		client: pahoClient,
		done:   make(chan struct{}),
	}
	go c.janitor()
	return c, nil
}

// janitor periodically removes topics whose consumers have all detached for longer
// than the grace period, closing their MQTT subscription and freeing the ring buffer.
func (c *client) janitor() {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.sweep()
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
		existing.reconcileFields(t.Fields)
		return existing
	}
	stored := newStreamTopic(t.Path, t.Interval, t.Fields, t.Window)
	stored.StreamingKey = t.StreamingKey
	c.topics.Map.Store(t.Key(), stored)
	return stored
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
