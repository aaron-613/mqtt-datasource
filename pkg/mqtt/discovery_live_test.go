package mqtt

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/require"
)

// TestLive_DiscoveryLifecycle exercises on-demand discovery against a live broker (start
// scripts/statspump_broker.js on :1884 first). Guarded by MQTT_LIVE=1 so it never runs
// in CI. It asserts the core claim: no root subscription at construction, a subscription
// (and populated registry) only after StartDiscovery, and that the cache survives a lapse.
func TestLive_DiscoveryLifecycle(t *testing.T) {
	if os.Getenv("MQTT_LIVE") == "" {
		t.Skip("set MQTT_LIVE=1 and run scripts/statspump_broker.js on :1884 to run this")
	}
	uri := envOr("MQTT_URI", "tcp://localhost:1884")
	root := envOr("MQTT_ROOT", "pump/#")

	ctx := context.Background()
	ci, err := NewClient(ctx, Options{URI: uri, DiscoveryMode: true, RootTopic: root}, backend.DataSourceInstanceSettings{})
	require.NoError(t, err)
	defer ci.Dispose()
	c := ci.(*client)

	// No firehose at construction: discovery is idle until an editor pings.
	require.Empty(t, c.ListTopics(), "no topics should be discovered before StartDiscovery")
	c.discMu.RLock()
	require.False(t, c.discoverySubscribed)
	c.discMu.RUnlock()

	// A ping subscribes and (retained samples arrive immediately) populates the registry.
	c.StartDiscovery()
	require.Eventually(t, func() bool { return len(c.ListTopics()) > 0 }, 5*time.Second, 200*time.Millisecond,
		"topics should appear after StartDiscovery")
	t.Logf("discovered after StartDiscovery: %v", c.ListTopics())
	c.discMu.RLock()
	require.True(t, c.discoverySubscribed)
	c.discMu.RUnlock()
	discoveredCount := len(c.ListTopics())

	// Force the lease into the past and reap: real paho Unsubscribe runs, the flag flips,
	// and the cache is retained so pick-lists still render.
	c.discMu.Lock()
	c.discoveryUntil = time.Now().Add(-time.Second)
	c.discMu.Unlock()
	c.reapDiscovery()
	c.discMu.RLock()
	require.False(t, c.discoverySubscribed, "lease lapsed -> unsubscribed")
	c.discMu.RUnlock()
	require.Len(t, c.ListTopics(), discoveredCount, "discovered cache survives the lapse")

	// A fresh ping re-subscribes without error.
	c.StartDiscovery()
	c.discMu.RLock()
	require.True(t, c.discoverySubscribed, "re-ping re-subscribes")
	c.discMu.RUnlock()
}
