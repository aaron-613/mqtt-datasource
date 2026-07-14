package mqtt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

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
