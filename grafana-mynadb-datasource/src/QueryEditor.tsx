import React, { ChangeEvent } from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { InlineField, Select, TextArea, InlineFieldRow, Input } from '@grafana/ui';
import { MynaDBDataSource } from './datasource';
import { MynaDBQuery, MynaDBDataSourceOptions } from './types';

type Props = QueryEditorProps<MynaDBDataSource, MynaDBQuery, MynaDBDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const queryModeOptions: Array<SelectableValue<string>> = [
    { label: 'SQL', value: 'sql', description: 'Standard SQL queries' },
    { label: 'InfluxQL', value: 'influxql', description: 'InfluxDB query language (coming soon)' },
  ];

  const formatOptions: Array<SelectableValue<string>> = [
    { label: 'Time series', value: 'time_series', description: 'For graphs and charts' },
    { label: 'Table', value: 'table', description: 'For tabular data' },
  ];

  const onQueryModeChange = (value: SelectableValue<string>) => {
    onChange({ ...query, queryMode: value.value as any });
  };

  const onFormatChange = (value: SelectableValue<string>) => {
    onChange({ ...query, format: value.value as any });
  };

  const onSqlChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, rawSql: event.target.value });
  };

  const onSqlBlur = () => {
    onRunQuery();
  };

  return (
    <div>
      <InlineFieldRow>
        <InlineField label="Query Mode" labelWidth={14} tooltip="Select query language">
          <Select
            options={queryModeOptions}
            value={query.queryMode || 'sql'}
            onChange={onQueryModeChange}
            width={20}
          />
        </InlineField>

        <InlineField label="Format" labelWidth={10} tooltip="Output format for visualization">
          <Select
            options={formatOptions}
            value={query.format || 'time_series'}
            onChange={onFormatChange}
            width={20}
          />
        </InlineField>
      </InlineFieldRow>

      {(query.queryMode === 'sql' || !query.queryMode) && (
        <>
          <InlineField label="SQL Query" labelWidth={14} grow tooltip="Enter your SQL query">
            <TextArea
              value={query.rawSql || ''}
              onChange={onSqlChange}
              onBlur={onSqlBlur}
              rows={6}
              placeholder="SELECT time_bucket('5m', time) as time, avg(value) as value&#10;FROM metrics&#10;WHERE $__timeFilter(time)&#10;GROUP BY time&#10;ORDER BY time"
            />
          </InlineField>

          <div style={{ marginTop: 8, marginLeft: 120, fontSize: 12, color: '#6e9fff' }}>
            <div style={{ marginBottom: 4 }}>
              <strong>Available macros:</strong>
            </div>
            <div>
              • <code>$__timeFilter(column)</code> - Replaced with time range filter (column &gt;= from
              AND column &lt;= to)
            </div>
            <div>
              • <code>$__interval</code> - Dynamic interval based on dashboard time range (e.g., '5m', '1h')
            </div>
            <div>
              • <code>$__from</code> - Start time in nanoseconds
            </div>
            <div>
              • <code>$__to</code> - End time in nanoseconds
            </div>
            <div style={{ marginTop: 4 }}>
              <strong>Example:</strong>
            </div>
            <code style={{ display: 'block', padding: 4, background: 'rgba(0, 0, 0, 0.2)', marginTop: 4 }}>
              SELECT time_bucket('$__interval', time) as time, avg(cpu_usage) as value
              <br />
              FROM cpu WHERE $__timeFilter(time) AND host = '$host'
              <br />
              GROUP BY time ORDER BY time
            </code>
          </div>
        </>
      )}

      {query.queryMode === 'influxql' && (
        <div style={{ marginTop: 16, padding: 16, background: 'rgba(255, 152, 0, 0.1)', borderRadius: 4 }}>
          <strong>InfluxQL Support Coming Soon</strong>
          <div style={{ marginTop: 8, fontSize: 12 }}>
            InfluxQL query language support is under development. Please use SQL mode for now.
          </div>
        </div>
      )}
    </div>
  );
}
