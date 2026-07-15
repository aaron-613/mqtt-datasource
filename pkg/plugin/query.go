package plugin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

// streamFlushInterval is the fixed cadence at which the backend flushes buffered
// messages to the live stream. It is deliberately independent of Grafana's
// range-derived query interval: baking that interval into the channel/buffer key
// would give every time range (and every zoom level) its own separate ring buffer,
// so a zoom or range change would land on an empty buffer and blank the panel.
const streamFlushInterval = time.Second

func (ds *MQTTDatasource) QueryData(_ context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := ds.query(q)
		response.Responses[q.RefID] = res
	}

	return response, nil
}

func (ds *MQTTDatasource) query(query backend.DataQuery) backend.DataResponse {
	var (
		t        mqtt.Topic
		response backend.DataResponse
	)

	if err := json.Unmarshal(query.JSON, &t); err != nil {
		return backend.ErrorResponseWithErrorSource(backend.DownstreamErrorf("failed to unmarshal query: %w", err))
	}

	if t.Path == "" {
		return backend.ErrorResponseWithErrorSource(backend.DownstreamErrorf("topic path is required"))
	}

	t.Interval = streamFlushInterval

	decoded, decodeErr := decodePath(t.Path)

	// Governance guardrail: when the datasource restricts topics, soft-reject a topic (or
	// wildcard pattern) that isn't under any configured root — return a notice instead of
	// subscribing. Not security; the editor also surfaces this inline.
	if ds.restrictTopics && decodeErr == nil && !mqtt.UnderRoot(decoded, ds.roots) {
		frame := data.NewFrame("")
		frame.SetMeta(&data.FrameMeta{Notices: []data.Notice{{
			Severity: data.NoticeSeverityWarning,
			Text:     fmt.Sprintf("Topic %q is outside the datasource's allowed root(s); not subscribing.", decoded),
		}}})
		response.Frames = append(response.Frames, frame)
		return response
	}

	// A wildcard topic (+/#) fans out into one series per matching concrete topic.
	if decodeErr == nil && mqtt.IsWildcard(decoded) {
		return ds.queryWildcard(&t, decoded)
	}

	// Single concrete topic: register it (field selection + ring buffer) and seed the
	// panel with recent buffered history so it opens populated and survives zoom
	// (seed-then-stream). On first load the buffer is empty, so the seed frame just
	// carries the channel and the live stream fills in from there.
	topic := ds.Client.EnsureTopic(&t)
	frame, err := topic.SeedFrame(log.DefaultLogger)
	if err != nil {
		return backend.ErrorResponseWithErrorSource(backend.DownstreamErrorf("failed to build seed frame: %w", err))
	}

	frame.SetMeta(&data.FrameMeta{
		Channel: path.Join(ds.channelPrefix, topic.Key()),
	})

	response.Frames = append(response.Frames, frame)
	return response
}

// queryWildcard expands a wildcard topic into one seed frame + channel per matching
// concrete topic (from the discovery registry), reusing the normal per-topic
// seed-then-stream machinery. Each series is labeled by its wildcard-matched segment.
func (ds *MQTTDatasource) queryWildcard(t *mqtt.Topic, pattern string) backend.DataResponse {
	var response backend.DataResponse
	logger := log.DefaultLogger

	type candidate struct {
		topic, matched string
	}
	var matches []candidate
	for _, concrete := range ds.Client.ListTopics() {
		matched, ok := mqtt.MatchTopic(pattern, concrete)
		if !ok {
			continue
		}
		if t.Filter != "" && !strings.Contains(concrete, t.Filter) {
			continue
		}
		matches = append(matches, candidate{concrete, matched})
	}

	total := len(matches)
	limit := ds.maxSeries
	if limit <= 0 {
		limit = defaultMaxSeries
	}
	truncated := total > limit
	if truncated {
		matches = matches[:limit]
	}
	logger.Debug("wildcard query", "pattern", pattern, "filter", t.Filter, "matched", total, "graphed", len(matches))

	if len(matches) == 0 {
		frame := data.NewFrame("")
		frame.SetMeta(&data.FrameMeta{Notices: []data.Notice{{
			Severity: data.NoticeSeverityInfo,
			Text:     fmt.Sprintf("No topics match %q yet (discovery may still be warming up, or the filter excludes all).", pattern),
		}}})
		response.Frames = append(response.Frames, frame)
		return response
	}

	for i, m := range matches {
		ct := &mqtt.Topic{
			Path:         encodePath(m.topic),
			StreamingKey: t.StreamingKey,
			Fields:       t.Fields,
			FieldAliases: t.FieldAliases,
			LabelSource:  t.LabelSource,
			LabelValue:   t.LabelValue,
			SeriesName:   m.matched,
			Interval:     t.Interval,
		}
		topic := ds.Client.EnsureTopic(ct)
		frame, err := topic.SeedFrame(logger)
		if err != nil {
			logger.Error("wildcard seed frame failed", "topic", m.topic, "error", err)
			continue
		}
		meta := &data.FrameMeta{Channel: path.Join(ds.channelPrefix, topic.Key())}
		if i == 0 && truncated {
			meta.Notices = []data.Notice{{
				Severity: data.NoticeSeverityWarning,
				Text:     fmt.Sprintf("Showing %d of %d matching topics — narrow with the filter.", len(matches), total),
			}}
		}
		frame.SetMeta(meta)
		response.Frames = append(response.Frames, frame)
	}
	return response
}

func decodePath(encoded string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(encoded)
	return string(b), err
}

func encodePath(topic string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topic))
}
