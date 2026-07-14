import React from 'react';
import { Input, InlineFieldRow, InlineField, TextArea, Combobox, MultiCombobox, ComboboxOption } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { MqttDataSourceOptions, MqttQuery } from './types';

type Props = QueryEditorProps<DataSource, MqttQuery, MqttDataSourceOptions>;

const LABEL_WIDTH = 10;

// Parse the multi-line/comma-separated Fields input into a clean list of leaf paths.
const parseFields = (raw: string): string[] =>
  raw
    .split(/[\n,]/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

const toOptions = (values: string[]): Array<ComboboxOption<string>> => values.map((v) => ({ label: v, value: v }));

const ClassicEditor = ({ query, onChange, onRunQuery }: Props) => (
  <>
    <InlineFieldRow>
      <InlineField label="Topic" labelWidth={LABEL_WIDTH} grow>
        <Input
          name="topic"
          required
          placeholder='e.g. "home/bedroom/temperature"'
          value={query.topic}
          onBlur={onRunQuery}
          onChange={(e) => onChange({ ...query, topic: e.currentTarget.value })}
        />
      </InlineField>
    </InlineFieldRow>
    <InlineFieldRow>
      <InlineField
        label="Fields"
        labelWidth={LABEL_WIDTH}
        grow
        tooltip="Optional. One JSON leaf path per line (dot notation, e.g. stats.totalTimeMs). Leave empty to graph every top-level key."
      >
        <TextArea
          name="fields"
          rows={3}
          placeholder={'stats.totalTimeMs\nstats.objectCount'}
          defaultValue={(query.fields ?? []).join('\n')}
          onBlur={(e) => {
            onChange({ ...query, fields: parseFields(e.currentTarget.value) });
            onRunQuery();
          }}
        />
      </InlineField>
    </InlineFieldRow>
  </>
);

const DiscoveryEditor = ({ query, onChange, onRunQuery, datasource }: Props) => {
  const loadTopics = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    const topics = await datasource.getResource<string[]>('topics');
    const needle = input.toLowerCase();
    return toOptions(topics.filter((t) => t.toLowerCase().includes(needle)));
  };

  const loadFields = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    if (!query.topic) {
      return [];
    }
    const fields = await datasource.getResource<string[]>('fields', { topic: query.topic });
    const needle = input.toLowerCase();
    return toOptions(fields.filter((f) => f.toLowerCase().includes(needle)));
  };

  const onTopicChange = (option: ComboboxOption<string> | null) => {
    // Changing topic invalidates the previously-selected fields/aliases.
    onChange({ ...query, topic: option?.value, fields: [], fieldAliases: {} });
    onRunQuery();
  };

  const onFieldsChange = (options: Array<ComboboxOption<string>>) => {
    const fields = options.map((o) => o.value);
    // Drop aliases for fields that are no longer selected.
    const aliases: Record<string, string> = {};
    for (const f of fields) {
      if (query.fieldAliases?.[f]) {
        aliases[f] = query.fieldAliases[f];
      }
    }
    onChange({ ...query, fields, fieldAliases: aliases });
    onRunQuery();
  };

  const onAliasChange = (path: string, alias: string) => {
    const aliases = { ...(query.fieldAliases ?? {}) };
    const trimmed = alias.trim();
    if (trimmed) {
      aliases[path] = trimmed;
    } else {
      delete aliases[path];
    }
    onChange({ ...query, fieldAliases: aliases });
    onRunQuery();
  };

  return (
    <>
      <InlineFieldRow>
        <InlineField label="Topic" labelWidth={LABEL_WIDTH} grow tooltip="Pick a discovered topic from the broker.">
          <Combobox
            options={loadTopics}
            value={query.topic ?? null}
            onChange={onTopicChange}
            placeholder="Select a topic"
            isClearable
            width="auto"
            minWidth={40}
          />
        </InlineField>
      </InlineFieldRow>
      <InlineFieldRow>
        <InlineField
          label="Fields"
          labelWidth={LABEL_WIDTH}
          grow
          tooltip="Pick which JSON leaf fields to graph (discovered from a sample message)."
        >
          <MultiCombobox
            options={loadFields}
            value={query.fields ?? []}
            onChange={onFieldsChange}
            placeholder={query.topic ? 'Select fields' : 'Select a topic first'}
            width={40}
          />
        </InlineField>
      </InlineFieldRow>
      {(query.fields ?? []).map((path) => (
        <InlineFieldRow key={path}>
          <InlineField label={path} labelWidth={28} grow tooltip="Optional legend alias for this field.">
            <Input
              placeholder="alias (optional)"
              defaultValue={query.fieldAliases?.[path] ?? ''}
              onBlur={(e) => onAliasChange(path, e.currentTarget.value)}
            />
          </InlineField>
        </InlineFieldRow>
      ))}
    </>
  );
};

export const QueryEditor = (props: Props) => {
  return props.datasource.discoveryMode ? <DiscoveryEditor {...props} /> : <ClassicEditor {...props} />;
};
