package mqtt

import (
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/stretchr/testify/require"
)

// These exercise the REAL *client Subscribe/GetTopic paths through fakePaho. They replace an
// earlier hand-written mockClient that had drifted from raw/view semantics (it keyed buffers by
// the full streaming path and "isolated" per streaming key — the opposite of the shared-raw model,
// so it asserted behavior the real client no longer has). Full-lifecycle coverage lives in
// harness_test.go; these pin the Subscribe/GetTopic contract itself.

func TestClient_Subscribe_InvalidReqPath(t *testing.T) {
	c := newWildcardTestClient(&fakePaho{})
	for _, bad := range []string{"invalid", "bad-interval/" + encodeTopic("a/b")} {
		_, err := c.Subscribe(bad, log.DefaultLogger)
		require.Error(t, err, "Subscribe(%q) should error", bad)
	}
}

func TestClient_Subscribe_DedupSameReqPath(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	req := "1s/" + encodeTopic("test/topic") + "/u/h/1"

	t1, err := c.Subscribe(req, log.DefaultLogger)
	require.NoError(t, err)
	t2, err := c.Subscribe(req, log.DefaultLogger)
	require.NoError(t, err)

	require.Same(t, t1, t2, "the same reqPath returns the same view instance")
	require.Equal(t, []string{"test/topic"}, fp.subscribed, "one broker subscription for the topic")
	n := 0
	c.topics.Range(func(_, _ any) bool { n++; return true })
	require.Equal(t, 1, n, "one view stored")
}

func TestClient_Subscribe_DistinctKeysShareOneSub(t *testing.T) {
	fp := &fakePaho{}
	c := newWildcardTestClient(fp)
	base := encodeTopic("test/topic")
	t1, err := c.Subscribe("1s/"+base+"/u1/h1/1", log.DefaultLogger)
	require.NoError(t, err)
	t2, err := c.Subscribe("1s/"+base+"/u2/h2/1", log.DefaultLogger)
	require.NoError(t, err)

	require.NotSame(t, t1, t2, "distinct streaming keys -> distinct views")
	require.Equal(t, base, t1.Path)
	require.Equal(t, base, t2.Path, "both views share the base64 topic Path")
	require.Equal(t, []string{"test/topic"}, fp.subscribed, "but exactly one shared broker subscription")
}

func TestClient_GetTopic(t *testing.T) {
	c := newWildcardTestClient(&fakePaho{})
	req := "2s/" + encodeTopic("test/topic") + "/s/k/1"

	_, found := c.GetTopic(req)
	require.False(t, found, "topic absent before subscribe")

	sub, err := c.Subscribe(req, log.DefaultLogger)
	require.NoError(t, err)
	got, found := c.GetTopic(req)
	require.True(t, found)
	require.Same(t, sub, got, "GetTopic returns the subscribed view instance")
}
