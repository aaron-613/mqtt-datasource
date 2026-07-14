package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"

	"github.com/grafana/mqtt-datasource/pkg/mqtt"
)

// Make sure MQTTDatasource implements required interfaces.
// This is important to do since otherwise we will only get a
// not implemented error response from plugin in runtime.
var (
	_ backend.QueryDataHandler      = (*MQTTDatasource)(nil)
	_ backend.CheckHealthHandler    = (*MQTTDatasource)(nil)
	_ backend.StreamHandler         = (*MQTTDatasource)(nil)
	_ backend.CallResourceHandler   = (*MQTTDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*MQTTDatasource)(nil)
)

// NewMQTTDatasource creates a new datasource instance.
func NewMQTTInstance(ctx context.Context, s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	settings, err := getDatasourceSettings(s)
	if err != nil {
		return nil, err
	}

	client, err := mqtt.NewClient(ctx, *settings, s)
	if err != nil {
		return nil, err
	}

	return NewMQTTDatasource(client, s.UID), nil
}

type MQTTDatasource struct {
	Client        mqtt.Client
	channelPrefix string

	// CallResourceHandler serves the discovery resource endpoints (/topics, /fields)
	// that the query editor's pick-lists fetch from.
	backend.CallResourceHandler
}

// NewMQTTDatasource creates a new datasource instance.
func NewMQTTDatasource(client mqtt.Client, uid string) *MQTTDatasource {
	ds := &MQTTDatasource{
		Client:        client,
		channelPrefix: path.Join("ds", uid),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/topics", ds.handleTopics)
	mux.HandleFunc("/fields", ds.handleFields)
	ds.CallResourceHandler = httpadapter.New(mux)
	return ds
}

// handleTopics returns the topics discovered on the root wildcard subscription.
func (ds *MQTTDatasource) handleTopics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, ds.Client.ListTopics())
}

// handleFields returns the flattened leaf paths of a discovered topic's sample
// payload (e.g. stats.total-time-ms), for the field multi-select. Expects the raw
// (non-base64) topic as the ?topic= query parameter.
func (ds *MQTTDatasource) handleFields(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	sample, ok := ds.Client.SampleFor(topic)
	if !ok {
		writeJSON(w, []string{})
		return
	}
	paths, err := mqtt.FlattenLeafPaths(sample)
	if err != nil || len(paths) == 0 {
		writeJSON(w, []string{})
		return
	}
	writeJSON(w, paths)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// Dispose here tells plugin SDK that plugin wants to clean up resources
// when a new instance created. As soon as datasource settings change detected
// by SDK old datasource instance will be disposed and a new one will be created
// using NewMQTTDatasource factory function.
func (ds *MQTTDatasource) Dispose() {
	ds.Client.Dispose()
}

func getDatasourceSettings(s backend.DataSourceInstanceSettings) (*mqtt.Options, error) {
	settings := &mqtt.Options{}

	if err := json.Unmarshal(s.JSONData, settings); err != nil {
		return nil, err
	}

	if password, exists := s.DecryptedSecureJSONData["password"]; exists {
		settings.Password = password
	}

	if tlsClientCert, exists := s.DecryptedSecureJSONData["tlsClientCert"]; exists {
		settings.TLSClientCert = tlsClientCert
	}

	if tlsClientKey, exists := s.DecryptedSecureJSONData["tlsClientKey"]; exists {
		settings.TLSClientKey = tlsClientKey
	}

	if tlsCACert, exists := s.DecryptedSecureJSONData["tlsCACert"]; exists {
		settings.TLSCACert = tlsCACert
	}

	return settings, nil
}
