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
  // For a wildcard topic: only graph matching concrete topics whose name contains this substring.
  filter?: string;
  // Series (object) label — which dimension names each series in the legend.
  labelSource?: 'topic' | 'payload' | 'custom';
  // Meaning depends on labelSource: topic level indices (e.g. "5" or "3,5"; blank = wildcard match),
  // a payload field path, or a literal string.
  labelValue?: string;
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
  // Cap on how many concrete topics a wildcard query fans out into (default 100).
  maxSeries?: number;
  // Restrict topics: scope queries to rootTopic. Out-of-scope topics are soft-rejected
  // (backend notice + editor warning). A tidiness guardrail, not security. Default off.
  restrictTopics?: boolean;
  // How long (seconds) a subscription + recent-history buffer is kept after a panel stops
  // viewing, so a zoom/pan/refresh reseeds instead of blanking. 0/unset = default (30s).
  graceSeconds?: number;
}

export interface MqttSecureJsonData {
  password?: string;
  tlsCACert?: string;
  tlsClientKey?: string;
  tlsClientCert?: string;
}
