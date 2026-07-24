import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceInstanceSettings,
  ScopedVars,
} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { MqttDataSourceOptions, MqttQuery } from './types';
import { Observable, from, switchMap } from 'rxjs';
import { getLiveStreamKey } from './streaming';

export class DataSource extends DataSourceWithBackend<MqttQuery, MqttDataSourceOptions> {
  readonly discoveryMode: boolean;
  readonly restrictTopics: boolean;
  // Non-wildcard prefixes parsed from the configured root topic(s), used by the query
  // editor to warn when a typed topic falls outside the allowed scope.
  readonly rootTopics: string[];

  constructor(instanceSettings: DataSourceInstanceSettings<MqttDataSourceOptions>) {
    super(instanceSettings);
    this.discoveryMode = !!instanceSettings.jsonData?.discoveryMode;
    this.restrictTopics = !!instanceSettings.jsonData?.restrictTopics;
    this.rootTopics = (instanceSettings.jsonData?.rootTopic ?? '')
      .split(',')
      .map((r) => r.trim())
      .filter((r) => r.length > 0)
      // Strip from the first wildcard so the guardrail is a plain prefix check.
      .map((r) => {
        const i = r.search(/[+#]/);
        return i >= 0 ? r.slice(0, i) : r;
      });
  }

  query(request: DataQueryRequest<MqttQuery>): Observable<DataQueryResponse> {
    // Add streamingKey to each target using RxJS operators
    return from(
      Promise.all(
        request.targets.map(async (target) => ({
          ...target,
          streamingKey: await getLiveStreamKey(this.uid, target.topic, target.fields),
        }))
      )
    ).pipe(
      switchMap((updatedTargets) => {
        const updatedRequest = {
          ...request,
          targets: updatedTargets,
        };
        return super.query(updatedRequest);
      })
    );
  }

  applyTemplateVariables(query: MqttQuery, scopedVars: ScopedVars, filters?: any[]): MqttQuery {
    const srv = getTemplateSrv();
    const resolvedQuery: MqttQuery = {
      ...query,
      refId: query.refId,
      topic: this.base64UrlSafeEncode(srv.replace(query.topic, scopedVars)),
    };
    // Interpolate the free-text query fields too, so dashboard variables work in them.
    if (query.filter) {
      // 'csv' format so a multi-value variable expands to comma-separated terms — which is exactly
      // the include/exclude filter's own syntax (so a multi-select var becomes an OR of includes).
      resolvedQuery.filter = srv.replace(query.filter, scopedVars, 'csv');
    }
    if (query.labelValue) {
      resolvedQuery.labelValue = srv.replace(query.labelValue, scopedVars);
    }
    return resolvedQuery;
  }

  // There are some restrictions to what characters are allowed to use in a Grafana Live channel:
  //
  //  https://github.com/grafana/grafana-plugin-sdk-go/blob/7470982de35f3b0bb5d17631b4163463153cc204/live/channel.go#L33
  //
  // To comply with these restrictions, the topic is encoded using URL-safe base64 encoding.
  // (RFC 4648; 5. Base 64 Encoding with URL and Filename Safe Alphabet)
  private base64UrlSafeEncode(input: string): string {
    return btoa(input).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }
}
