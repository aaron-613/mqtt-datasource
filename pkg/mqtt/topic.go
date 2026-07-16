package mqtt

import (
	"encoding/base64"
	"encoding/json"
	"path"
	"sort"
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

	// stream holds concurrency/framing state (the VIEW layer). It is a pointer so Topic
	// stays copyable (some tests copy Topic by value); it is created for topics the
	// client manages for streaming. The raw ring buffer + MQTT subscription live in the
	// shared rawTopic (the RAW layer) that stream.raw points to — one per MQTT topic,
	// shared across every field-selection/query of that topic.
	stream *streamState
}

// rawTopic is the shared RAW layer, keyed by Path (base64 topic) alone. It owns the single
// ring buffer for that MQTT topic, shared across all field-selections/queries (views). Each
// view frames this buffer through its own framer + watermark. (Subscription ownership and
// refcounting move onto rawTopic in the follow-up commit; for now the per-view streamState
// still owns the paho subscription.)
type rawTopic struct {
	mu       sync.Mutex
	path     string        // base64 topic == Topic.Path; the raws-map key
	window   time.Duration // ring-buffer retention window
	messages []Message     // THE shared ring buffer (also the source for streamed deltas)

	// pahoSubscribed is true when THIS topic has its own concrete paho subscription open (opened
	// for a directly-typed concrete view). A topic graphed only via a wildcard has no own sub —
	// its messages arrive on the wildcard subscription and are demuxed here by dispatch. Both
	// feeds land in this one buffer; dispatch appends once per delivered message either way.
	pahoSubscribed bool

	// refCount is the number of active RunStream consumers (across all views/field-selections
	// of this topic). detachedAt is when it last dropped to 0, for janitor reaping past grace.
	refCount   int
	detachedAt time.Time
}

func newRawTopic(path string, window time.Duration) *rawTopic {
	if window <= 0 {
		window = defaultWindow
	}
	return &rawTopic{path: path, window: window}
}

// append adds a message to the shared ring buffer and trims by window and cap.
func (r *rawTopic) append(m Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, m)
	r.trimLocked(m.Timestamp)
}

func (r *rawTopic) trimLocked(now time.Time) {
	if r.window > 0 {
		cutoff := now.Add(-r.window)
		drop := 0
		for drop < len(r.messages) && r.messages[drop].Timestamp.Before(cutoff) {
			drop++
		}
		if drop > 0 {
			r.messages = append(r.messages[:0], r.messages[drop:]...)
		}
	}
	if len(r.messages) > maxBufferedMessages {
		r.messages = append(r.messages[:0], r.messages[len(r.messages)-maxBufferedMessages:]...)
	}
}

// snapshot returns a copy of the entire buffer (for seeding).
func (r *rawTopic) snapshot() []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Message, len(r.messages))
	copy(out, r.messages)
	return out
}

// since returns a copy of the messages strictly newer than watermark (for streaming deltas).
// Messages are appended in timestamp order, so binary-search the first one after the watermark
// and copy the tail — O(log n + delta) instead of scanning the whole buffer every tick.
func (r *rawTopic) since(watermark time.Time) []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := sort.Search(len(r.messages), func(i int) bool {
		return r.messages[i].Timestamp.After(watermark)
	})
	if i >= len(r.messages) {
		return nil
	}
	delta := make([]Message, len(r.messages)-i)
	copy(delta, r.messages[i:])
	return delta
}

// streamState is the per-query VIEW layer: a framer over the shared raw buffer, plus this
// view's own watermark. Subscription/refcount/coverage all live on the shared rawTopic now.
type streamState struct {
	mu        sync.Mutex
	framer    *framer
	watermark time.Time // timestamp of the last message emitted to THIS view's stream
	raw       *rawTopic // shared raw layer (buffer + subscription) for this view's Path
	// enumeratedBy is the wildcard pattern (if any) whose queryWildcard produced THIS view;
	// empty for a directly-typed concrete query. A wildcard-enumerated view rides that pattern's
	// shared subscription; a concrete view always opens its own subscription and is never
	// absorbed into a wildcard firehose.
	enumeratedBy string
	// wildcardRef is the pattern this view currently holds a reference on (via attachWildcardExact),
	// so Unsubscribe releases exactly that reference. Empty when the view uses its own concrete sub.
	wildcardRef string
}

// newStreamTopic builds a Topic with view + a private raw layer initialized. Callers that
// want the SHARED raw for a Path (EnsureTopic) overwrite stream.raw afterwards; the private
// raw here serves standalone Topics (the RunStream fallback and tests).
func newStreamTopic(topicPath string, interval time.Duration, fields []string, window time.Duration) *Topic {
	return &Topic{
		Path:     topicPath,
		Interval: interval,
		Fields:   fields,
		stream:   &streamState{framer: newFramer(fields...), raw: newRawTopic(topicPath, window)},
	}
}

// ensureStream lazily initializes view + raw state for Topics created as literals
// (e.g. in tests). Client-managed topics are always built via newStreamTopic so
// this is a no-op for them.
func (t *Topic) ensureStream() {
	if t.stream == nil {
		t.stream = &streamState{framer: newFramer(t.Fields...)}
	}
	// Note: stream.raw is initialized lazily under stream.mu by currentRaw (and StreamDelta),
	// never here — reading it unlocked would race Subscribe re-pointing it.
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

// currentRaw returns the view's shared raw layer, read under stream.mu because Subscribe may
// re-point it to a freshly-created raw if the previous one was reaped (see ensureRawAttached).
func (t *Topic) currentRaw() *rawTopic {
	t.ensureStream()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()
	if t.stream.raw == nil {
		t.stream.raw = newRawTopic(t.Path, 0)
	}
	return t.stream.raw
}

// appendMessage adds a message to the shared raw ring buffer.
func (t *Topic) appendMessage(m Message) {
	t.currentRaw().append(m)
}

// bufferLen returns the number of messages currently in the shared raw buffer.
func (t *Topic) bufferLen() int {
	r := t.currentRaw()
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.messages)
}

// SeedFrame frames the entire retained buffer, for QueryData to seed a panel with
// recent history (seed-then-stream). It advances the stream watermark to the last
// seeded message so the subsequent live stream does not re-send seeded rows.
func (t *Topic) SeedFrame(logger log.Logger) (*data.Frame, error) {
	// Snapshot the shared raw buffer first (releasing raw.mu), then take the view lock for
	// framer + watermark. The two locks are never held simultaneously.
	raw := t.currentRaw()
	msgs := raw.snapshot()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()

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
	if t.stream.raw == nil {
		t.stream.raw = newRawTopic(t.Path, 0)
	}
	wm := t.stream.watermark
	raw := t.stream.raw
	t.stream.mu.Unlock()

	// Read the delta from the shared raw buffer without holding the view lock.
	delta := raw.since(wm)
	if len(delta) == 0 {
		return nil, false, nil
	}

	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()
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
// the frontend's channel encoding and the Topic.Path key used to route into the raw layer.
func encodeTopic(topic string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topic))
}
