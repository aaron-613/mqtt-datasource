package mqtt

import (
	"fmt"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"
)

// completedToken is a paho.Token that reports success immediately, for the fake client.
type completedToken struct{}

func (completedToken) Wait() bool                     { return true }
func (completedToken) WaitTimeout(time.Duration) bool { return true }
func (completedToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (completedToken) Error() error { return nil }

// fakePaho records subscribe/unsubscribe calls without touching a broker. Embedding the
// interface means any un-overridden method panics if the code under test reaches it.
type fakePaho struct {
	paho.Client
	subscribed   []string
	unsubscribed []string
}

func (f *fakePaho) Subscribe(topic string, _ byte, _ paho.MessageHandler) paho.Token {
	f.subscribed = append(f.subscribed, topic)
	return completedToken{}
}

func (f *fakePaho) Unsubscribe(topics ...string) paho.Token {
	f.unsubscribed = append(f.unsubscribed, topics...)
	return completedToken{}
}

func TestSplitRoots(t *testing.T) {
	for _, c := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"mqtt/PUMP/#", []string{"mqtt/PUMP/#"}},
		{"mqtt/PUMP/#, sensors/#", []string{"mqtt/PUMP/#", "sensors/#"}},
		{" a/# , , b/# ", []string{"a/#", "b/#"}},
	} {
		require.Equal(t, c.want, SplitRoots(c.in), "SplitRoots(%q)", c.in)
	}
}

func TestClient_Discovery_Lease(t *testing.T) {
	fp := &fakePaho{}
	c := &client{
		client:         fp,
		discovered:     make(map[string][]byte),
		discoveryMode:  true,
		discoveryRoots: []string{"mqtt/PUMP/#", "sensors/#"},
	}
	// Seed the cache so we can assert it survives a lapse.
	c.recordDiscovered("mqtt/PUMP/x", []byte(`{"a":1}`))

	// First ping subscribes to each root and sets a future lease.
	c.StartDiscovery()
	require.True(t, c.discoverySubscribed)
	require.True(t, c.discoveryUntil.After(time.Now()))
	require.Equal(t, []string{"mqtt/PUMP/#", "sensors/#"}, fp.subscribed)

	// A second ping while still subscribed only bumps the lease — no re-subscribe.
	c.StartDiscovery()
	require.Len(t, fp.subscribed, 2, "no duplicate subscribe while lease is live")

	// Lease not yet lapsed: reap is a no-op.
	c.reapDiscovery()
	require.True(t, c.discoverySubscribed)
	require.Empty(t, fp.unsubscribed)

	// Force the lease into the past: reap unsubscribes but keeps the cache.
	c.discMu.Lock()
	c.discoveryUntil = time.Now().Add(-time.Second)
	c.discMu.Unlock()
	c.reapDiscovery()
	require.False(t, c.discoverySubscribed)
	require.Equal(t, []string{"mqtt/PUMP/#", "sensors/#"}, fp.unsubscribed)
	_, ok := c.SampleFor("mqtt/PUMP/x")
	require.True(t, ok, "discovered cache survives a lapse")

	// A ping after lapse re-subscribes.
	c.StartDiscovery()
	require.True(t, c.discoverySubscribed)
	require.Len(t, fp.subscribed, 4, "re-subscribe after lapse")
}

func TestClient_Discovery_Disabled(t *testing.T) {
	fp := &fakePaho{}
	// discoveryMode off, and separately no roots: both are no-ops.
	c := &client{client: fp, discovered: make(map[string][]byte), discoveryRoots: []string{"a/#"}}
	c.StartDiscovery()
	require.False(t, c.discoverySubscribed)
	require.Empty(t, fp.subscribed)

	c2 := &client{client: fp, discovered: make(map[string][]byte), discoveryMode: true}
	c2.StartDiscovery()
	require.False(t, c2.discoverySubscribed)
	require.Empty(t, fp.subscribed)
}

func TestClient_Discovery_Registry(t *testing.T) {
	c := &client{discovered: make(map[string][]byte)}

	c.recordDiscovered("pump/system/pollerB", []byte(`{"b":2}`))
	c.recordDiscovered("pump/system/pollerA", []byte(`{"a":1}`))
	c.recordDiscovered("pump/system/pollerA", []byte(`{"a":9}`)) // update keeps latest

	require.Equal(t, []string{"pump/system/pollerA", "pump/system/pollerB"}, c.ListTopics())

	sample, ok := c.SampleFor("pump/system/pollerA")
	require.True(t, ok)
	require.JSONEq(t, `{"a":9}`, string(sample))

	_, ok = c.SampleFor("pump/system/nope")
	require.False(t, ok)
}

func TestClient_Discovery_FIFOEviction(t *testing.T) {
	c := &client{discovered: make(map[string][]byte)}
	for i := 0; i < maxDiscoveredTopics; i++ {
		c.recordDiscovered(fmt.Sprintf("t/%d", i), []byte("{}"))
	}
	require.Len(t, c.ListTopics(), maxDiscoveredTopics)
	_, ok := c.SampleFor("t/0")
	require.True(t, ok, "oldest present before overflow")

	// Overflow by one: the oldest first-seen topic is evicted, the new one admitted.
	c.recordDiscovered("t/newest", []byte("{}"))
	require.Len(t, c.ListTopics(), maxDiscoveredTopics, "stays capped")
	_, ok = c.SampleFor("t/0")
	require.False(t, ok, "oldest should be evicted (FIFO)")
	_, ok = c.SampleFor("t/newest")
	require.True(t, ok, "newest should be admitted")

	// Updating an existing topic must not grow the registry or reorder eviction.
	c.recordDiscovered("t/1", []byte(`{"x":1}`))
	require.Len(t, c.ListTopics(), maxDiscoveredTopics)
}

func TestClient_Discovery_CopiesPayload(t *testing.T) {
	c := &client{discovered: make(map[string][]byte)}
	buf := []byte(`{"x":1}`)
	c.recordDiscovered("t", buf)
	buf[0] = 'X' // mutating the caller's buffer (as paho may reuse it) must not corrupt the stored sample

	sample, ok := c.SampleFor("t")
	require.True(t, ok)
	require.Equal(t, `{"x":1}`, string(sample))
}
