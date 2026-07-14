package plugin

import (
	"context"
	"encoding/json"
	"path"
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

	// Register the topic (its field selection + ring buffer) and seed the panel with
	// any recent buffered history so it opens populated and survives zoom
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
