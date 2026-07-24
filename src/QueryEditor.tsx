import React, { useCallback, useEffect, useState } from 'react';
import { Alert, Button, Input, InlineFieldRow, InlineField, TextArea, Combobox, ComboboxOption, IconButton } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import { DataSource } from './datasource';
import { MqttDataSourceOptions, MqttQuery } from './types';
import { isWildcard, matchesTopic, matchesFilter } from './wildcard';

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
          tooltip="Optional. One JSON leaf path per line (slash notation, e.g. stats/totalTimeMs). Leave empty to graph every top-level key."
        >
          <TextArea
            name="fields"
            rows={3}
            placeholder={'stats/totalTimeMs\nstats/objectCount'}
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
  const topicIsWildcard = isWildcard(topicDraft);

  // Keep-alive: while this editor is mounted, ping /topics so the backend keeps the
  // discovery subscription open (it lapses ~30s after the last ping). The returned list
  // feeds both the total-count hint and the per-pattern "matches N topics" hint below.
  const [topics, setTopics] = useState<string[] | null>(null);
  const ping = useCallback(async () => {
    try {
      const list = await datasource.getResource<string[]>('topics');
      // Only re-render when the list actually changed, so the 10s keep-alive doesn't churn the
      // editor (and disturb in-progress typing) on every tick.
      setTopics((prev) =>
        prev && prev.length === list.length && prev.every((t, i) => t === list[i]) ? prev : list
      );
    } catch {
      // best-effort keep-alive; ignore transient failures
    }
  }, [datasource]);

  // How many discovered topics the typed wildcard pattern would match (intersected with the
  // committed Filter substring, mirroring the backend queryWildcard pipeline — case-sensitive,
  // like strings.Contains). null = don't show (concrete topic, or discovery still warming up).
  // Resolve dashboard variables so the hint reflects what will actually graph (e.g. a
  // ${topicFilter} variable in the Filter box), matching applyTemplateVariables' 'csv' format.
  const resolvedFilter = getTemplateSrv().replace(query.filter ?? '', undefined, 'csv');
  const matchCount =
    topicIsWildcard && topics && topics.length > 0
      ? topics.filter((t) => matchesTopic(topicDraft, t) && matchesFilter(t, resolvedFilter)).length
      : null;

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
    const list = await datasource.getResource<string[]>('topics');
    const needle = input.toLowerCase();
    return toOptions(list.filter((t) => t.toLowerCase().includes(needle)));
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

  // Append a blank row to type a (not-yet-discovered) path into. It commits — or self-removes if
  // left empty — on blur (commitFieldAt), so no query runs until there's an actual path.
  const addBlankField = () => {
    onChange({ ...query, fields: [...fields, ''] });
  };

  // Commit a manually-typed field path on blur/Enter. Empty clears the row; unchanged is a no-op
  // (so tabbing through doesn't re-run the query); otherwise reuse setFieldAt (migrates the alias key).
  const commitFieldAt = (index: number, raw: string) => {
    const next = raw.trim();
    if (!next) {
      removeFieldAt(index);
      return;
    }
    if (next === fields[index]) {
      return;
    }
    setFieldAt(index, next);
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
        {topicIsWildcard && (
          <InlineField
            label="Filter"
            labelWidth={10}
            tooltip="Comma-separated substrings — a topic matches if it contains any of them. Prefix a term with ! to exclude it. Case-sensitive. Supports dashboard variables (e.g. ${topicFilter}); a multi-value variable expands to comma terms."
          >
            <Input
              placeholder="e.g. include, !exclude"
              defaultValue={query.filter ?? ''}
              onBlur={(e) => onFilterChange(e.currentTarget.value)}
              width={26}
            />
          </InlineField>
        )}
        {matchCount !== null && (
          <span style={{ alignSelf: 'center', marginLeft: 8, whiteSpace: 'nowrap', opacity: 0.75 }}>
            matches {matchCount} {matchCount === 1 ? 'topic' : 'topics'}
          </span>
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
        <InlineField
          label="Field"
          labelWidth={9}
          tooltip="Pick a discovered field to add it as a row below. Needs a topic set first."
        >
          <Combobox
            options={loadFields}
            value={null}
            onChange={(o) => addField(o?.value)}
            placeholder={query.topic ? 'add a discovered field…' : 'set a topic first'}
            width={40}
            disabled={!query.topic}
          />
        </InlineField>
        <IconButton name="sync" tooltip="Refresh discovered topics" onClick={ping} />
        {topics !== null && (
          <span style={{ alignSelf: 'center', marginLeft: 8, whiteSpace: 'nowrap', opacity: 0.75 }}>
            {rootPrefixes.length > 0 ? `Discovering under ${rootPrefixes.join(', ')} — ` : 'Discovering — '}
            {topics.length} topics
          </span>
        )}
      </InlineFieldRow>

      {fields.map((f, i) => (
        // Keyed by index+value: stable while typing (value only changes on blur-commit, so no
        // remount mid-type), but remounts with the right defaultValue on add/remove/commit.
        <InlineFieldRow key={`${i}-${f}`}>
          <InlineField
            label={i === 0 ? 'Fields' : ' '}
            labelWidth={LABEL_WIDTH}
            tooltip="JSON leaf path (slash notation), e.g. stats/total-time-ms — type it, or use the Field browser above to add a discovered one."
          >
            <Input
              placeholder="e.g. stats/total-time-ms"
              defaultValue={f}
              onKeyDown={commitOnEnter}
              onBlur={(e) => commitFieldAt(i, e.currentTarget.value)}
              width={40}
            />
          </InlineField>
          <IconButton name="trash-alt" tooltip="Remove field" onClick={() => removeFieldAt(i)} />
          <InlineField label="Alias" labelWidth={9} tooltip="Optional legend name for this field.">
            <Input
              placeholder="(optional)"
              defaultValue={query.fieldAliases?.[f] ?? ''}
              onBlur={(e) => onAliasChange(f, e.currentTarget.value)}
              width={24}
            />
          </InlineField>
        </InlineFieldRow>
      ))}

      <InlineFieldRow>
        <InlineField label={fields.length === 0 ? 'Fields' : ' '} labelWidth={LABEL_WIDTH}>
          <Button variant="secondary" fill="text" icon="plus" onClick={addBlankField}>
            Add field
          </Button>
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
          label="Value"
          labelWidth={9}
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
