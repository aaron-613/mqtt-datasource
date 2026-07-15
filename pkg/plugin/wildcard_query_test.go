package plugin

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
	"github.com/stretchr/testify/require"
)

func wildcardQuery(topic, filter string) backend.DataQuery {
	body, _ := json.Marshal(map[string]any{
		"topic":  base64.RawURLEncoding.EncodeToString([]byte(topic)),
		"filter": filter,
	})
	return backend.DataQuery{RefID: "A", JSON: body}
}

func TestQueryWildcard_EnumeratesFromEnsureWildcard(t *testing.T) {
	newDS := func(seen []string) *MQTTDatasource {
		mc := &mockMQTTClient{
			topics:        map[string]*mqtt.Topic{},
			subscriptions: map[string]bool{},
			wildcardSeen:  seen,
		}
		return NewMQTTDatasource(mc, "uid")
	}

	t.Run("matches expand to one frame/channel each", func(t *testing.T) {
		ds := newDS([]string{"a/b/c", "a/d/c", "x/y/z"})
		res := ds.query(wildcardQuery("a/+/c", ""))
		require.NoError(t, res.Error)
		require.Len(t, res.Frames, 2, "a/b/c and a/d/c match a/+/c; x/y/z does not")
	})

	t.Run("substring filter narrows matches", func(t *testing.T) {
		ds := newDS([]string{"a/b/c", "a/d/c"})
		res := ds.query(wildcardQuery("a/+/c", "d"))
		require.NoError(t, res.Error)
		require.Len(t, res.Frames, 1, "only a/d/c contains the filter substring")
	})

	t.Run("no matches yet -> single warming-up notice frame", func(t *testing.T) {
		ds := newDS(nil)
		res := ds.query(wildcardQuery("a/+/c", ""))
		require.NoError(t, res.Error)
		require.Len(t, res.Frames, 1)
		require.NotNil(t, res.Frames[0].Meta)
		require.Len(t, res.Frames[0].Meta.Notices, 1)
	})
}
