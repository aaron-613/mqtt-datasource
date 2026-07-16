package plugin

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
	"github.com/stretchr/testify/require"
)

func restrictQueryFor(topic string) backend.DataQuery {
	body, _ := json.Marshal(map[string]any{"topic": base64.RawURLEncoding.EncodeToString([]byte(topic))})
	return backend.DataQuery{RefID: "A", JSON: body}
}

func newRestrictDS(restrict bool, roots []string) (*MQTTDatasource, *mockMQTTClient) {
	mc := &mockMQTTClient{topics: map[string]*mqtt.Topic{}}
	ds := NewMQTTDatasource(mc, "uid")
	ds.restrictTopics = restrict
	ds.roots = roots
	return ds, mc
}

func TestQuery_RestrictTopics(t *testing.T) {
	roots := []string{"mqtt/PUMP/#"}

	t.Run("out-of-scope topic returns a notice and does not subscribe", func(t *testing.T) {
		ds, mc := newRestrictDS(true, roots)
		res := ds.query(restrictQueryFor("mqtt/OTHER/x"))
		require.NoError(t, res.Error)
		require.Len(t, res.Frames, 1)
		require.NotNil(t, res.Frames[0].Meta)
		notices := res.Frames[0].Meta.Notices
		require.Len(t, notices, 1)
		require.Equal(t, data.NoticeSeverityWarning, notices[0].Severity)
		require.Contains(t, notices[0].Text, "outside the datasource's allowed root(s)")
		require.Empty(t, mc.topics, "out-of-scope topic must not be registered/subscribed")
	})

	t.Run("in-scope topic subscribes normally with no notice", func(t *testing.T) {
		ds, mc := newRestrictDS(true, roots)
		res := ds.query(restrictQueryFor("mqtt/PUMP/solace/x"))
		require.NoError(t, res.Error)
		require.Len(t, res.Frames, 1)
		require.Nil(t, res.Frames[0].Meta.Notices)
		require.Len(t, mc.topics, 1, "in-scope topic is registered")
	})

	t.Run("restrict off is fully permissive (classic parity)", func(t *testing.T) {
		ds, mc := newRestrictDS(false, roots)
		res := ds.query(restrictQueryFor("mqtt/OTHER/x"))
		require.NoError(t, res.Error)
		require.Len(t, mc.topics, 1, "no restriction -> topic registered")
	})
}
