package plugin

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

// Integration tests to verify end-to-end streaming key functionality

func TestStreamingKeyIntegration_TopicUniqueness(t *testing.T) {
	// Test that the complete flow from query to topic creation maintains uniqueness

	// Simulate query data with streaming keys
	query1JSON, _ := json.Marshal(map[string]interface{}{
		"topic":        "sensor/temperature",
		"streamingKey": "user1/hash123/org456",
	})

	query2JSON, _ := json.Marshal(map[string]interface{}{
		"topic":        "sensor/temperature",   // Same MQTT topic
		"streamingKey": "user2/hash456/org456", // Different user, same org
	})

	query3JSON, _ := json.Marshal(map[string]interface{}{
		"topic":        "sensor/temperature",   // Same MQTT topic
		"streamingKey": "user1/hash123/org789", // Same user, different org
	})

	// Create queries
	query1 := backend.DataQuery{
		JSON:     query1JSON,
		Interval: 1 * time.Second,
		RefID:    "A",
	}

	query2 := backend.DataQuery{
		JSON:     query2JSON,
		Interval: 1 * time.Second,
		RefID:    "B",
	}

	query3 := backend.DataQuery{
		JSON:     query3JSON,
		Interval: 1 * time.Second,
		RefID:    "C",
	}

	// Create datasource instance with a mock client (QueryData now seeds from the
	// client's ring buffer, so a client must be present).
	ds := &MQTTDatasource{
		channelPrefix: "ds/test-uid",
		Client: &mockMQTTClient{
			topics: make(map[string]*mqtt.Topic),
		},
	}

	// Process queries
	resp1 := ds.query(query1)
	resp2 := ds.query(query2)
	resp3 := ds.query(query3)

	// Verify no errors
	if resp1.Error != nil {
		t.Errorf("Query 1 failed: %v", resp1.Error)
	}
	if resp2.Error != nil {
		t.Errorf("Query 2 failed: %v", resp2.Error)
	}
	if resp3.Error != nil {
		t.Errorf("Query 3 failed: %v", resp3.Error)
	}

	// Extract channel paths
	channel1 := resp1.Frames[0].Meta.Channel
	channel2 := resp2.Frames[0].Meta.Channel
	channel3 := resp3.Frames[0].Meta.Channel

	// Verify all channels are different
	if channel1 == channel2 {
		t.Errorf("Expected different channels for different users, but got same: %s", channel1)
	}
	if channel1 == channel3 {
		t.Errorf("Expected different channels for different orgs, but got same: %s", channel1)
	}
	if channel2 == channel3 {
		t.Errorf("Expected different channels for different combinations, but got same: %s", channel2)
	}

	// Verify channel format
	expectedChannel1 := "ds/test-uid/1s/sensor/temperature/user1/hash123/org456"
	if channel1 != expectedChannel1 {
		t.Errorf("Expected channel1 %s, got %s", expectedChannel1, channel1)
	}

	expectedChannel2 := "ds/test-uid/1s/sensor/temperature/user2/hash456/org456"
	if channel2 != expectedChannel2 {
		t.Errorf("Expected channel2 %s, got %s", expectedChannel2, channel2)
	}

	expectedChannel3 := "ds/test-uid/1s/sensor/temperature/user1/hash123/org789"
	if channel3 != expectedChannel3 {
		t.Errorf("Expected channel3 %s, got %s", expectedChannel3, channel3)
	}
}

// Mock MQTT client for plugin-layer tests (query.go channel building, wildcard enumeration,
// restrict guardrail). A stub for the mqtt.Client interface — the REAL client's Subscribe/raw/view
// behavior is covered in pkg/mqtt (client_test.go + harness_test.go), not here.
type mockMQTTClient struct {
	topics       map[string]*mqtt.Topic
	wildcardSeen []string // concrete topics EnsureWildcard should return
}

func (m *mockMQTTClient) GetTopic(reqPath string) (*mqtt.Topic, bool) {
	topic, found := m.topics[reqPath]
	return topic, found
}

func (m *mockMQTTClient) EnsureTopic(t *mqtt.Topic) *mqtt.Topic {
	if existing, ok := m.topics[t.Key()]; ok {
		return existing
	}
	m.topics[t.Key()] = t
	return t
}

func (m *mockMQTTClient) ListTopics() []string            { return nil }
func (m *mockMQTTClient) SampleFor(string) ([]byte, bool) { return nil, false }
func (m *mockMQTTClient) StartDiscovery()                 {}
func (m *mockMQTTClient) EnsureWildcard(string) []string  { return m.wildcardSeen }

func (m *mockMQTTClient) IsConnected() bool {
	return true
}

func (m *mockMQTTClient) Subscribe(reqPath string, _ log.Logger) (*mqtt.Topic, error) {
	if topic, exists := m.topics[reqPath]; exists {
		return topic, nil
	}
	// Parse reqPath the way the real client does: interval / base64-topic / streaming-key...
	chunks := strings.Split(reqPath, "/")
	if len(chunks) < 2 {
		return nil, nil
	}
	topic := &mqtt.Topic{Path: chunks[1], StreamingKey: strings.Join(chunks[2:], "/"), Interval: time.Second}
	m.topics[reqPath] = topic
	return topic, nil
}

func (m *mockMQTTClient) Unsubscribe(reqPath string, _ log.Logger) error {
	delete(m.topics, reqPath)
	return nil
}

func (m *mockMQTTClient) Dispose() {
	m.topics = make(map[string]*mqtt.Topic)
}

