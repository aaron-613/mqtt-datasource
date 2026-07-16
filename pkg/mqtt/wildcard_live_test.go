package mqtt

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/stretchr/testify/require"
)

// TestLive_WildcardLifecycle exercises the Option-B wildcard path against a live broker (start
// scripts/statspump_broker.js on :1884 first). Guarded by MQTT_LIVE=1. It proves the core
// claims: with discovery DISABLED, a wildcard panel's own subscription still discovers its
// topics (Issue 1 fixed), exactly one wildcard subscription is tracked (Issue 2), and that one
// subscription feeds the per-series ring buffers.
func TestLive_WildcardLifecycle(t *testing.T) {
	if os.Getenv("MQTT_LIVE") == "" {
		t.Skip("set MQTT_LIVE=1 and run scripts/statspump_broker.js on :1884 to run this")
	}
	uri := envOr("MQTT_URI", "tcp://localhost:1884")
	pattern := envOr("MQTT_WILDCARD", "pump/system/+")

	ctx := context.Background()
	// Discovery OFF on purpose: graphing must not depend on it.
	ci, err := NewClient(ctx, Options{URI: uri, DiscoveryMode: false}, backend.DataSourceInstanceSettings{})
	require.NoError(t, err)
	defer ci.Dispose()
	c := ci.(*client)

	// The panel's own wildcard subscription discovers matching topics (no discovery running).
	seen := c.EnsureWildcard(pattern)
	require.NotEmpty(t, seen, "wildcard subscription should observe topics with discovery disabled")
	t.Logf("wildcard seen under %q: %v", pattern, seen)

	// Exactly one wildcard subscription is tracked for the pattern.
	c.wildMu.RLock()
	_, ok := c.wildcards[pattern]
	n := len(c.wildcards)
	c.wildMu.RUnlock()
	require.True(t, ok)
	require.Equal(t, 1, n, "one wildcard subscription, not one-per-topic")

	// That single subscription feeds a per-series buffer (attach-only, no per-topic sub).
	// EnumeratedBy mirrors what queryWildcard tags, scoping coverage to this pattern.
	c.EnsureTopic(&Topic{Path: encodeTopic(seen[0]), Interval: time.Second, StreamingKey: "k", EnumeratedBy: pattern})
	reqPath := "1s/" + encodeTopic(seen[0]) + "/k"
	top, err := c.Subscribe(reqPath, log.DefaultLogger)
	require.NoError(t, err)
	// The enumerated series rides the wildcard pattern's shared sub and opens no per-topic sub.
	raw := top.stream.raw
	raw.mu.Lock()
	ownSub := raw.pahoSubscribed
	raw.mu.Unlock()
	require.False(t, ownSub, "a wildcard-enumerated topic must not open its own subscription")
	top.stream.mu.Lock()
	ref := top.stream.wildcardRef
	top.stream.mu.Unlock()
	require.Equal(t, pattern, ref, "the view rides the wildcard pattern's shared subscription")

	require.Eventually(t, func() bool {
		return top.bufferLen() > 0
	}, 5*time.Second, 100*time.Millisecond, "wildcard subscription should feed the per-series buffer")
}
