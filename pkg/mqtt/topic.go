package mqtt

import (
	"encoding/base64"
	"encoding/json"
	"path"
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
	Path         string        `json:"topic"`
	StreamingKey string        `json:"streamingKey,omitempty"`
	Fields       []string      `json:"fields,omitempty"` // selected leaf paths (dot notation); empty = classic top-level extraction
	Interval     time.Duration `json:"-"`
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
func (t *Topic) reconcileFields(fields []string) {
	t.ensureStream()
	t.stream.mu.Lock()
	defer t.stream.mu.Unlock()
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

// applyLabels attaches identifying labels to the selected value fields so multiple
// queries in one panel are distinguishable and the legend can be templated
// (e.g. Display name = ${__field.labels.name} or ${__field.labels.topic}). Only
// applied in selection mode so classic-mode frames (and their golden tests) are
// untouched. Called with the topic's stream lock held.
func (t *Topic) applyLabels(frame *data.Frame, msgs []Message) {
	if frame == nil || len(t.Fields) == 0 {
		return
	}
	labels := data.Labels{}
	if decoded, err := base64.RawURLEncoding.DecodeString(t.Path); err == nil {
		labels["topic"] = string(decoded)
	}
	// Best-effort: the payload's top-level "name" (constant per topic, e.g.
	// "show stats client" / "show queue *").
	if len(msgs) > 0 {
		var root map[string]interface{}
		if json.Unmarshal(msgs[len(msgs)-1].Value, &root) == nil {
			if name, ok := root["name"].(string); ok && name != "" {
				labels["name"] = name
			}
		}
	}
	for _, f := range frame.Fields {
		if f.Name == "Time" {
			continue
		}
		f.Labels = labels
	}
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
