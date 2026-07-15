import React, { useCallback, useEffect, useState } from 'react';
import { Alert, Input, InlineFieldRow, InlineField, TextArea, Combobox, ComboboxOption, IconButton } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { MqttDataSourceOptions, MqttQuery } from './types';

type Props = QueryEditorProps<DataSource, MqttQuery, MqttDataSourceOptions>;

const LABEL_WIDTH = 14;

// How often the open editor pings /topics to keep the backend's discovery lease warm
// (backend discoveryLeaseTTL is 30s, so 10s stays comfortably inside it).
const DISCOVERY_PING_MS = 10000;

// Parse the multi-line/comma-separated Fields input into a clean list of leaf paths.
const parseFields = (raw: string): string[] =>
  raw
    .split(/[\n,]/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

const toOptions = (values: string[]): Array<ComboboxOption<string>> => values.map((v) => ({ label: v, value: v }));

const LABEL_SOURCES: Array<ComboboxOption<string>> = [
  { label: 'Topic level', value: 'topic' },
  { label: 'Payload field', value: 'payload' },
  { label: 'Custom', value: 'custom' },
];

// useTopicDraft gives the Topic input commit-on-blur behavior: typing updates only a local
// draft, so the query model — and therefore any MQTT subscribe — changes ONLY when the user
// commits (blur or Enter), never on each keystroke. Without this, Grafana re-runs the query on
// every model change, leaving a trail of intermediate subscriptions on the broker while typing.
const useTopicDraft = (query: MqttQuery, onChange: (q: MqttQuery) => void, onRunQuery: () => void) => {
  const [topicDraft, setTopicDraft] = useState(query.topic ?? '');
  useEffect(() => {
    // Sync when the topic changes externally (e.g. picking from Browse, or a dashboard reload).
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setTopicDraft(query.topic ?? '');
  }, [query.topic]);
  const commitTopic = () => {
    const next = topicDraft.trim();
    if (next !== (query.topic ?? '')) {
      onChange({ ...query, topic: next });
    }
    onRunQuery();
  };
  return { topicDraft, setTopicDraft, commitTopic };
};

// commitOnEnter blurs the input when Enter is pressed, so Enter commits like leaving the field.
const commitOnEnter = (e: React.KeyboardEvent<HTMLInputElement>) => {
  if (e.key === 'Enter') {
    e.currentTarget.blur();
  }
};

const ClassicEditor = ({ query, onChange, onRunQuery }: Props) => {
  const { topicDraft, setTopicDraft, commitTopic } = useTopicDraft(query, onChange, onRunQuery);
  return (
    <>
      <InlineFieldRow>
        <InlineField label="Topic" labelWidth={LABEL_WIDTH} grow>
          <Input
            name="topic"
            required
            placeholder='e.g. "home/bedroom/temperature"'
            value={topicDraft}
            onChange={(e) => setTopicDraft(e.currentTarget.value)}
            onKeyDown={commitOnEnter}
            onBlur={commitTopic}
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
};

const DiscoveryEditor = ({ query, onChange, onRunQuery, datasource }: Props) => {
  const fields = query.fields ?? [];

  const { topicDraft, setTopicDraft, commitTopic } = useTopicDraft(query, onChange, onRunQuery);
  const isWildcard = /[+#]/.test(topicDraft);

  // Keep-alive: while this editor is mounted, ping /topics so the backend keeps the
  // discovery subscription open (it lapses ~30s after the last ping). The returned count
  // feeds the hint line below.
  const [topicCount, setTopicCount] = useState<number | null>(null);
  const ping = useCallback(async () => {
    try {
      const topics = await datasource.getResource<string[]>('topics');
      setTopicCount(topics.length);
    } catch {
      // best-effort keep-alive; ignore transient failures
    }
  }, [datasource]);

  useEffect(() => {
    // ping() only setState()s after an await (the /topics fetch), so this is a genuine
    // external-system sync, not the synchronous cascading render the rule guards against.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    ping();
    const id = setInterval(ping, DISCOVERY_PING_MS);
    return () => clearInterval(id);
  }, [ping]);

  const rootPrefixes = datasource.rootTopics;
  const outsideRoot =
    datasource.restrictTopics &&
    !!topicDraft &&
    rootPrefixes.length > 0 &&
    !rootPrefixes.some((p) => topicDraft.startsWith(p));

  const loadTopics = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    const topics = await datasource.getResource<string[]>('topics');
    const needle = input.toLowerCase();
    return toOptions(topics.filter((t) => t.toLowerCase().includes(needle)));
  };

  const loadFields = async (input: string): Promise<Array<ComboboxOption<string>>> => {
    if (!query.topic) {
      return [];
    }
    let all: string[] = [];
    try {
      all = await datasource.getResource<string[]>('fields', { topic: query.topic });
    } catch {
      all = [];
    }
    // Always include already-selected fields so they render even while discovery is warming
    // (or if the topic isn't in the registry yet) — otherwise the picker looks blanked.
    const merged = Array.from(new Set([...(query.fields ?? []), ...all]));
    const needle = input.toLowerCase();
    return toOptions(merged.filter((f) => f.toLowerCase().includes(needle)));
  };

  // Picking a discovered topic replaces the topic but keeps the field selection — a field
  // still present in the new topic keeps working; a stale one can be changed/removed.
  const onTopicPicked = (option: ComboboxOption<string> | null) => {
    if (!option?.value) {
      return;
    }
    onChange({ ...query, topic: option.value });
    onRunQuery();
  };

  const onFilterChange = (value: string) => {
    onChange({ ...query, filter: value.trim() || undefined });
    onRunQuery();
  };

  const labelSource = query.labelSource ?? 'topic';
  const onLabelSourceChange = (option: ComboboxOption<string> | null) => {
    onChange({ ...query, labelSource: (option?.value as MqttQuery['labelSource']) ?? 'topic', labelValue: undefined });
    onRunQuery();
  };
  const onLabelValueChange = (value: string) => {
    onChange({ ...query, labelValue: value.trim() || undefined });
    onRunQuery();
  };
  const labelValuePlaceholder =
    labelSource === 'topic'
      ? 'e.g. 3 or 3,5 or -2 (blank == wildcard match, or last level)'
      : labelSource === 'payload'
        ? 'e.g. name or userId or system'
        : 'text';

  const setFieldAt = (index: number, field?: string) => {
    if (!field) {
      return;
    }
    const next = [...fields];
    const prev = next[index];
    next[index] = field;
    const aliases = { ...(query.fieldAliases ?? {}) };
    if (prev && prev !== field && aliases[prev]) {
      aliases[field] = aliases[prev];
      delete aliases[prev];
    }
    onChange({ ...query, fields: next, fieldAliases: aliases });
    onRunQuery();
  };

  const removeFieldAt = (index: number) => {
    const removed = fields[index];
    const aliases = { ...(query.fieldAliases ?? {}) };
    delete aliases[removed];
    onChange({ ...query, fields: fields.filter((_, i) => i !== index), fieldAliases: aliases });
    onRunQuery();
  };

  const addField = (field?: string) => {
    if (!field || fields.includes(field)) {
      return;
    }
    onChange({ ...query, fields: [...fields, field] });
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
        <InlineField
          label="Topic"
          labelWidth={LABEL_WIDTH}
          grow
          tooltip="Type a concrete topic or a wildcard pattern (+ / #). Use Browse to pick a discovered topic."
        >
          <Input
            value={topicDraft}
            placeholder="topic or wildcard, e.g. mqtt/PUMP/+/POLLER_STAT/VPN/queue_rates"
            onChange={(e) => setTopicDraft(e.currentTarget.value)}
            onKeyDown={commitOnEnter}
            onBlur={commitTopic}
          />
        </InlineField>
        {isWildcard && (
          <InlineField label="Filter" labelWidth={8} tooltip="Only graph matching topics whose name contains this text.">
            <Input
              placeholder="substring"
              defaultValue={query.filter ?? ''}
              onBlur={(e) => onFilterChange(e.currentTarget.value)}
              width={22}
            />
          </InlineField>
        )}
      </InlineFieldRow>

      {outsideRoot && (
        <InlineFieldRow>
          <Alert severity="warning" title="Topic outside the allowed root(s)">
            This topic is not under the datasource&apos;s restricted root(s) ({rootPrefixes.join(', ')}). It will not be
            subscribed until it&apos;s in scope.
          </Alert>
        </InlineFieldRow>
      )}

      <InlineFieldRow>
        <InlineField
          label="Browse"
          labelWidth={LABEL_WIDTH}
          tooltip="Pick a discovered topic to fill the Topic field. Start typing to filter the list."
        >
          <Combobox
            options={loadTopics}
            value={null}
            onChange={onTopicPicked}
            placeholder="type to filter discovered topics…"
            width={50}
          />
        </InlineField>
        <IconButton name="sync" tooltip="Refresh discovered topics" onClick={ping} />
        {topicCount !== null && (
          <span style={{ alignSelf: 'center', marginLeft: 8, whiteSpace: 'nowrap', opacity: 0.75 }}>
            {rootPrefixes.length > 0 ? `Discovering under ${rootPrefixes.join(', ')} — ` : 'Discovering — '}
            {topicCount} topics
          </span>
        )}
      </InlineFieldRow>

      {fields.map((f, i) => (
        <InlineFieldRow key={`${f}-${i}`}>
          <InlineField label={i === 0 ? 'Fields' : ' '} labelWidth={LABEL_WIDTH}>
            <Combobox options={loadFields} value={f} onChange={(o) => setFieldAt(i, o?.value)} createCustomValue width={40} />
          </InlineField>
          <InlineField label="Alias" labelWidth={7} tooltip="Optional legend name for this field.">
            <Input
              placeholder="(optional)"
              defaultValue={query.fieldAliases?.[f] ?? ''}
              onBlur={(e) => onAliasChange(f, e.currentTarget.value)}
              width={24}
            />
          </InlineField>
          <IconButton name="trash-alt" tooltip="Remove field" onClick={() => removeFieldAt(i)} />
        </InlineFieldRow>
      ))}

      <InlineFieldRow>
        <InlineField label={fields.length === 0 ? 'Fields' : ' '} labelWidth={LABEL_WIDTH}>
          <Combobox
            options={loadFields}
            value={null}
            onChange={(o) => addField(o?.value)}
            placeholder={query.topic ? '+ add field' : 'set a topic first'}
            width={40}
          />
        </InlineField>
      </InlineFieldRow>

      <InlineFieldRow>
        <InlineField
          label="Series label"
          labelWidth={LABEL_WIDTH}
          tooltip="What names each series (the object dimension). Per-field Alias handles the metric dimension; the legend combines them."
        >
          <Combobox options={LABEL_SOURCES} value={labelSource} onChange={onLabelSourceChange} width={18} />
        </InlineField>
        <InlineField
          label=""
          labelWidth={1}
          grow
          tooltip="Topic: level indices (0-based; negatives from the end); blank = the wildcard-matched level(s). Payload: a field path. Custom: a literal string."
        >
          <Input
            key={labelSource}
            placeholder={labelValuePlaceholder}
            defaultValue={query.labelValue ?? ''}
            onBlur={(e) => onLabelValueChange(e.currentTarget.value)}
          />
        </InlineField>
      </InlineFieldRow>
    </>
  );
};

export const QueryEditor = (props: Props) => {
  return props.datasource.discoveryMode ? <DiscoveryEditor {...props} /> : <ClassicEditor {...props} />;
};
