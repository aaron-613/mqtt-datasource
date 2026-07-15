import React, { SyntheticEvent } from 'react';

import {
  DataSourcePluginOptionsEditorProps,
  onUpdateDatasourceJsonDataOption,
  onUpdateDatasourceSecureJsonDataOption,
  updateDatasourcePluginJsonDataOption,
  updateDatasourcePluginResetOption,
} from '@grafana/data';
import { ConfigSection, DataSourceDescription } from '@grafana/plugin-ui';
import { Alert, Field, Input, SecretInput, SecureSocksProxySettings, Switch } from '@grafana/ui';
import { Divider } from './Divider';
import { TLSSecretsConfig } from './TLSConfig';
import { MqttDataSourceOptions, MqttSecureJsonData } from './types';
import { config } from '@grafana/runtime';

export const ConfigEditor = (props: DataSourcePluginOptionsEditorProps<MqttDataSourceOptions, MqttSecureJsonData>) => {
  const { options, onOptionsChange } = props;
  const jsonData = options.jsonData;

  const onResetPassword = () => {
    updateDatasourcePluginResetOption(props, 'password');
  };

  const onSwitchChanged = (property: keyof MqttDataSourceOptions) => {
    return (event: SyntheticEvent<HTMLInputElement>) => {
      updateDatasourcePluginJsonDataOption(props, property, event.currentTarget.checked);
    };
  };

  const WIDTH_LONG = 40;

  return (
    <>
      <DataSourceDescription
        dataSourceName="MQTT"
        docsLink="https://grafana.com/grafana/plugins/grafana-mqtt-datasource/?tab=overview"
        hasRequiredFields={true}
      />

      <Divider />

      <ConfigSection title="Connection">
        <Field label="URI" required>
          <Input
            width={WIDTH_LONG}
            name="URI"
            type="text"
            value={jsonData.uri || ''}
            onChange={onUpdateDatasourceJsonDataOption(props, 'uri')}
            placeholder="TCP (tcp://), TLS (tls://), or WebSocket (ws://)"
          />
        </Field>
      </ConfigSection>

      <Field label="Client ID" description="If not set, a random client ID is used.">
        <Input
          width={WIDTH_LONG}
          name="Client ID"
          type="text"
          value={jsonData.clientID || ''}
          onChange={onUpdateDatasourceJsonDataOption(props, 'clientID')}
        />
      </Field>

      <Divider />

      <ConfigSection title="Discovery">
        <Field
          label="Discovery mode"
          description="Subscribe to a root topic wildcard (while a query editor is open) and offer topic/field pick-lists in the query editor instead of hand-typed topics."
        >
          <Switch onChange={onSwitchChanged('discoveryMode')} value={jsonData.discoveryMode || false} />
        </Field>

        {jsonData.discoveryMode ? (
          <Alert severity="info" title="Discovery ingests live data while editing">
            While a query editor is open, discovery subscribes to the configured root topic(s) and receives every
            matching message to build the topic/field pick-lists. A broad root (e.g. <code>#</code>) on a busy broker can
            pull a large volume of traffic — and at a high rate if many topics match. Scope the root(s) as narrowly as
            practical.
          </Alert>
        ) : null}

        <Field
          label="Restrict topics"
          description="Scope queries to the root topic(s) below. Topics outside the root(s) are soft-rejected with a warning. A tidiness guardrail, not a security boundary."
        >
          <Switch onChange={onSwitchChanged('restrictTopics')} value={jsonData.restrictTopics || false} />
        </Field>

        {jsonData.discoveryMode || jsonData.restrictTopics ? (
          <>
            <Field
              label="Root topic(s)"
              description='The scope for discovery and/or restriction — wildcard filter(s), comma-separated for multiple, e.g. "mqtt/PUMP/#, sensors/#".'
            >
              <Input
                width={WIDTH_LONG}
                name="Root topic"
                type="text"
                value={jsonData.rootTopic || ''}
                onChange={onUpdateDatasourceJsonDataOption(props, 'rootTopic')}
                placeholder="e.g. mqtt/PUMP/#, sensors/#"
              />
            </Field>
            <Field label="Max series" description="Max concrete topics a wildcard query graphs (default 100).">
              <Input
                width={WIDTH_LONG}
                name="Max series"
                type="number"
                value={jsonData.maxSeries?.toString() ?? ''}
                onChange={(e) => {
                  const n = parseInt(e.currentTarget.value, 10);
                  updateDatasourcePluginJsonDataOption(props, 'maxSeries', isNaN(n) ? undefined : n);
                }}
                placeholder="100"
              />
            </Field>
          </>
        ) : null}
      </ConfigSection>

      <Divider />

      <ConfigSection title="Authentication">
        <Field label="Username">
          <Input
            width={WIDTH_LONG}
            value={jsonData.username || ''}
            placeholder="Username"
            onChange={onUpdateDatasourceJsonDataOption(props, 'username')}
          />
        </Field>

        <Field label="Password">
          <SecretInput
            width={WIDTH_LONG}
            placeholder="Password"
            isConfigured={options.secureJsonFields && options.secureJsonFields.password}
            onReset={onResetPassword}
            onBlur={onUpdateDatasourceSecureJsonDataOption(props, 'password')}
          />
        </Field>

        <Field
          label="Use TLS Client Auth"
          description="Enables TLS authentication using client cert configured in secure json data."
        >
          <Switch onChange={onSwitchChanged('tlsAuth')} value={jsonData.tlsAuth || false} />
        </Field>

        <Field
          label="Skip TLS Verification"
          description="When enabled, skips verification of the MQTT server's TLS certificate chain and host name."
        >
          <Switch onChange={onSwitchChanged('tlsSkipVerify')} value={jsonData.tlsSkipVerify || false} />
        </Field>

        <Field label="With CA Cert" description="Needed for verifying servers with self-signed TLS Certs.">
          <Switch onChange={onSwitchChanged('tlsAuthWithCACert')} value={jsonData.tlsAuthWithCACert || false} />
        </Field>
      </ConfigSection>

      {jsonData.tlsAuth || jsonData.tlsAuthWithCACert ? (
        <>
          <Divider />

          <ConfigSection title="TLS Configuration">
            {jsonData.tlsAuth || jsonData.tlsAuthWithCACert ? (
              <TLSSecretsConfig
                showCACert={jsonData.tlsAuthWithCACert}
                showKeyPair={jsonData.tlsAuth}
                editorProps={props}
                labelWidth={WIDTH_LONG}
              />
            ) : null}
          </ConfigSection>
        </>
      ) : null}

      {config.secureSocksDSProxyEnabled && (
        <SecureSocksProxySettings options={options} onOptionsChange={onOptionsChange} />
      )}
    </>
  );
};
