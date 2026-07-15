package mqtt

import (
	"encoding/base64"
	"encoding/json"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// defaultWindow is how much recent history the ring buffer retains when a topic
// doesn't specify its own window. maxBufferedMessages is a hard safety cap so a
// fast/high-cardinality topic can't grow the buffer without bound.
const (
	defaultWindow       = 15 * time.Minute
	maxBufferedMessages = 20000
)

type Message struct {
	Timestamp time.Time
	Value     []byte
}

// Topic represents a MQTT topic subscription.
type Topic struct {
	Path         string            `json:"topic"`
	StreamingKey string            `json:"streamingKey,omitempty"`
	Fields       []string          `json:"fields,omitempty"`       // selected leaf paths (dot notation); empty = classic top-level extraction
	FieldAliases map[string]string `json:"fieldAliases,omitempty"` // leaf path -> display-name alias for the legend
	Filter       string            `json:"filter,omitempty"`       // wildcard substring filter (query-time only)
	LabelSource  string            `json:"labelSource,omitempty"`  // series-label source: "topic" (default) | "payload" | "custom"
	LabelValue   string            `json:"labelValue,omitempty"`   // meaning depends on LabelSource: level indices / payload path / literal
	SeriesName   string            `json:"-"`                      // wildcard-matched segment(s); the topic-source default when LabelValue is empty
	EnumeratedBy string            `json:"-"`                      // wildcard pattern that enumerated this series (scopes coverage); empty for classic queries
	Interval     time.Duration     `json:"-"`
	Window       time.Duration `json:"-"` // ring buffer retention window
	Messages     []Message     `json:"-"` // retained ring buffer (also the source for streamed deltas)

	// stream holds concurrency/buffering state. It is a pointer so Topic stays
	// copyable (some tests copy Topic by value); it is created for topics the
	// client manages for streaming.
	stream *streamState
}

type streamState struct {
	mu             sync.Mutex
	framer         *framer
	watermark      time.Time // timestamp of the last message emitted to the live stream
	pahoSubscribed bool      // whether an MQTT subscription is currently open for this topic
	attachedCount  int       // number of active RunStream consumers
	detachedAt     time.Time // when attachedCount last dropped to 0 (for janitor cleanup)
	// enumeratedBy is the wildcard pattern (if any) whose queryWildcard produced this series;
	// it scopes coverage so the series attaches only to that pattern's shared subscription.
	// coveredByPattern is set (to that filter) once the series is actually attached to the
	// shared wildcard subscription instead of its own per-topic subscription; empty for classic
	// single-topic subscriptions. Used to release the wildcard sub's refcount on detach.
	enumeratedBy     string
	coveredByPattern string
}

// newStreamTopic builds a Topic with buffering/streaming state initialized.
func newStreamTopic(topicPath string, interval time.Duration, fields []string, window time.Duration) *Topic {
	if window <= 0 {
		window = defaultWindow
	}
	return &Topic{
		Path:     topicPath,
		Interval: interval,
		Fields:   fields,
		Window:   window,
		stream:   &streamState{framer: newFramer(fields...)},
	}
}

// ensureStream lazily initializes stream state for Topics created as literals
// (e.g. in tests). Client-managed topics are always built via newStreamTopic so
// this is a no-op for them.
func (t *Topic) ensureStream() {
	if t.stream == nil {
		if t.Window <= 0 {
			t.Window = defaultWindow
		}
		t.stream = &streamState{framer: newFramer(t.Fields...)}
	}
}

// reconcileFields ensures the topic's framer matches the requested field selection.
// A topic can be created without fields by the RunStream fallback (which has no access
// to the query's field list) if a Live subscription races ahead of QueryData; when
// QueryData later runs with fields, this brings the framer into line. The raw ring
// buffer is retained and simply re-framed on the next Seed/Stream call.
func (t *Topic) reconcileFields(fields []string, aliases map[string]string, seriesName, labelSource, labelValue string) {
	t.ensureStream()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()
	// aliases, series name and label config only affect display (applied in applyLabels), no framer rebuild
	t.FieldAliases = aliases
	t.SeriesName = seriesName
	t.LabelSource = labelSource
	t.LabelValue = labelValue
	if sameStrings(t.Fields, fields) {
		return
	}
	t.Fields = fields
	t.stream.framer = newFramer(fields...)
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Key returns the key for the topic: interval, path, and streaming key joined.
// Field selection is NOT part of the key (it travels in the query JSON); the
// frontend folds the selected fields into the streaming-key hash so different
// selections already map to distinct keys.
func (t *Topic) Key() string {
	return path.Join(t.Interval.String(), t.Path, t.StreamingKey)
}

// appendMessage adds a message to the ring buffer and trims by window and cap.
func (t *Topic) appendMessage(m Message) {
	t.ensureStream()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()
	t.Messages = append(t.Messages, m)
	t.trimLocked(m.Timestamp)
}

func (t *Topic) trimLocked(now time.Time) {
	if t.Window > 0 {
		cutoff := now.Add(-t.Window)
		drop := 0
		for drop < len(t.Messages) && t.Messages[drop].Timestamp.Before(cutoff) {
			drop++
		}
		if drop > 0 {
			t.Messages = append(t.Messages[:0], t.Messages[drop:]...)
		}
	}
	if len(t.Messages) > maxBufferedMessages {
		t.Messages = append(t.Messages[:0], t.Messages[len(t.Messages)-maxBufferedMessages:]...)
	}
}

// SeedFrame frames the entire retained buffer, for QueryData to seed a panel with
// recent history (seed-then-stream). It advances the stream watermark to the last
// seeded message so the subsequent live stream does not re-send seeded rows.
func (t *Topic) SeedFrame(logger log.Logger) (*data.Frame, error) {
	t.ensureStream()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()

	msgs := make([]Message, len(t.Messages))
	copy(msgs, t.Messages)
	if len(msgs) > 0 {
		t.stream.watermark = msgs[len(msgs)-1].Timestamp
	}
	frame, err := t.stream.framer.toFrame(msgs, logger)
	t.applyLabels(frame, msgs)
	return frame, err
}

// StreamDelta frames only the messages that have arrived since the last emit and
// advances the watermark. Returns ok=false when there is nothing new to send.
func (t *Topic) StreamDelta(logger log.Logger) (*data.Frame, bool, error) {
	t.ensureStream()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()

	var delta []Message
	for _, m := range t.Messages {
		if m.Timestamp.After(t.stream.watermark) {
			delta = append(delta, m)
		}
	}
	if len(delta) == 0 {
		return nil, false, nil
	}
	t.stream.watermark = delta[len(delta)-1].Timestamp
	frame, err := t.stream.framer.toFrame(delta, logger)
	t.applyLabels(frame, delta)
	return frame, true, err
}

// applyLabels attaches identifying labels and a composed display name to the selected
// value fields. It models two dimensions: the OBJECT (which matched thing — the series
// label, from a configurable source) and the METRIC (which field — the per-field alias).
// The legend composes them: single field -> object label; many fields -> object · metric;
// no object label -> metric (alias) or the raw field name. Only applied in selection mode
// so classic-mode golden tests are untouched. Called with the topic's stream lock held.
func (t *Topic) applyLabels(frame *data.Frame, msgs []Message) {
	if frame == nil || len(t.Fields) == 0 {
		return
	}
	decodedTopic := ""
	if decoded, err := base64.RawURLEncoding.DecodeString(t.Path); err == nil {
		decodedTopic = string(decoded)
	}

	labels := data.Labels{}
	if decodedTopic != "" {
		labels["topic"] = decodedTopic
	}
	objectLabel := t.objectLabel(decodedTopic, msgs)
	if objectLabel != "" {
		labels["series"] = objectLabel
	}

	multiField := len(t.Fields) > 1
	for _, f := range frame.Fields {
		if f.Name == "Time" {
			continue
		}
		f.Labels = labels

		var display string
		switch {
		case objectLabel == "":
			// No object dimension: fall back to the metric alias, else the field name.
			display = t.FieldAliases[f.Name]
		case multiField:
			metric := t.FieldAliases[f.Name]
			if metric == "" {
				metric = lastSegment(f.Name)
			}
			display = objectLabel + " · " + metric
		default:
			display = objectLabel
		}
		if display != "" {
			if f.Config == nil {
				f.Config = &data.FieldConfig{}
			}
			f.Config.DisplayNameFromDS = display
		}
	}
}

// objectLabel computes the series (object) label from the configured source.
func (t *Topic) objectLabel(decodedTopic string, msgs []Message) string {
	switch t.LabelSource {
	case "custom":
		return t.LabelValue
	case "payload":
		field := t.LabelValue
		if field == "" {
			field = "name"
		}
		if len(msgs) > 0 {
			var root interface{}
			if json.Unmarshal(msgs[len(msgs)-1].Value, &root) == nil {
				if v, ok := lookupPath(root, field); ok {
					return stringifyLeaf(v)
				}
			}
		}
		return ""
	default: // "topic" (and unset) — the opinionated default
		if t.LabelValue == "" {
			// Blank = the wildcard-matched level(s) when there's a wildcard, else the last
			// level of a concrete topic (equivalent to index -1).
			if t.SeriesName != "" {
				return t.SeriesName
			}
			segments := strings.Split(decodedTopic, "/")
			if len(segments) > 0 {
				return segments[len(segments)-1]
			}
			return ""
		}
		return joinLevels(strings.Split(decodedTopic, "/"), t.LabelValue)
	}
}

// joinLevels picks topic segments by a comma-separated index spec (0-based; negatives
// count from the end, so -1 = last) and joins them with "_".
func joinLevels(segments []string, spec string) string {
	var picked []string
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		i, err := strconv.Atoi(part)
		if err != nil {
			continue
		}
		if i < 0 {
			i += len(segments)
		}
		if i >= 0 && i < len(segments) {
			picked = append(picked, segments[i])
		}
	}
	return strings.Join(picked, "_")
}

// stringifyLeaf renders a decoded JSON scalar as a label string.
func stringifyLeaf(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return ""
}

// lastSegment returns the final dot-separated segment of a leaf path
// (e.g. "stats.total-time-ms" -> "total-time-ms").
func lastSegment(path string) string {
	if i := strings.LastIndex(path, "."); i >= 0 {
		return path[i+1:]
	}
	return path
}

// TopicMap is a thread-safe map of topics
type TopicMap struct {
	sync.Map
}

// Load returns the topic for the given topic key.
func (tm *TopicMap) Load(key string) (*Topic, bool) {
	t, ok := tm.Map.Load(key)
	if !ok {
		return nil, false
	}

	topic, ok := t.(*Topic)
	return topic, ok
}

// AddMessage adds a message to every topic whose MQTT path matches.
func (tm *TopicMap) AddMessage(path string, message Message) {
	tm.Range(func(key, t any) bool {
		topic, ok := t.(*Topic)
		if !ok {
			return false
		}
		if topic.Path == path {
			topic.appendMessage(message)
		}
		return true
	})
}

// HasSubscription returns true if the topic map has a subscription for the given path.
func (tm *TopicMap) HasSubscription(path string) bool {
	found := false

	tm.Range(func(key, t any) bool {
		topic, ok := t.(*Topic)
		if !ok {
			return true // this shouldn't happen, but continue iterating
		}

		if topic.Path == path {
			found = true
			return false // topic found, stop iterating
		}

		return true // continue iterating
	})

	return found
}

// Store stores the topic in the map.
func (tm *TopicMap) Store(t *Topic) {
	tm.Map.Store(t.Key(), t)
}

// Delete deletes the topic for the given key.
func (tm *TopicMap) Delete(key string) {
	tm.Map.Delete(key)
}

// decodeTopic decodes an MQTT topic name from base64 URL encoding.
//
// There are some restrictions to what characters are allowed to use in a Grafana Live channel:
//
//	https://github.com/grafana/grafana-plugin-sdk-go/blob/7470982de35f3b0bb5d17631b4163463153cc204/live/channel.go#L33
//
// To comply with these restrictions, the topic is encoded using URL-safe base64
// encoding. (RFC 4648; 5. Base 64 Encoding with URL and Filename Safe Alphabet)
func decodeTopic(topicPath string, logger log.Logger) (string, error) {
	chunks := strings.Split(topicPath, "/")
	topic := chunks[0]
	logger.Debug("Decoding MQTT topic name", "encodedTopic", topic)
	decoded, err := base64.RawURLEncoding.DecodeString(topic)

	if err != nil {
		return "", err
	}

	return string(decoded), nil
}

// encodeTopic is the inverse of decodeTopic for a single topic name: URL-safe base64, matching
// the frontend's channel encoding and the Topic.Path key used by TopicMap.AddMessage.
func encodeTopic(topic string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topic))
}
