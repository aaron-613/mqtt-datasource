import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface MqttQuery extends DataQuery {
  topic?: string;
  stream?: boolean;
  streamingKey?: string;
  // Selected leaf paths (dot notation, e.g. "stats.totalTimeMs") to extract from the
  // JSON payload. Empty/undefined = classic behavior (every top-level key becomes a field).
  fields?: string[];
  // Optional per-field display-name alias for the legend, keyed by leaf path.
  fieldAliases?: Record<string, string>;
}

export interface MqttDataSourceOptions extends DataSourceJsonData {
  uri: string;
  username?: string;
  clientID?: string;
  tlsAuth: boolean;
  tlsAuthWithCACert: boolean;
  tlsSkipVerify: boolean;
  // Discovery mode: subscribe to rootTopic (a wildcard) and offer topic/field
  // pick-lists in the query editor instead of hand-typed topics.
  discoveryMode?: boolean;
  rootTopic?: string;
}

export interface MqttSecureJsonData {
  password?: string;
  tlsCACert?: string;
  tlsClientKey?: string;
  tlsClientCert?: string;
}
