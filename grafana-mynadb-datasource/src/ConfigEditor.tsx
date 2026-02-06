import React, { ChangeEvent } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input, FieldSet } from '@grafana/ui';
import { MynaDBDataSourceOptions } from './types';

type Props = DataSourcePluginOptionsEditorProps<MynaDBDataSourceOptions>;

export function ConfigEditor({ options, onOptionsChange }: Props) {
  const onDatabaseChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...options.jsonData,
        database: event.target.value,
      },
    });
  };

  return (
    <FieldSet label="MynaDB Connection">
      <InlineField
        label="Default Database"
        labelWidth={20}
        tooltip="Default catalog/database name to use for queries. Leave empty to use 'default'."
      >
        <Input
          value={options.jsonData.database || ''}
          onChange={onDatabaseChange}
          placeholder="default"
          width={40}
        />
      </InlineField>

      <div style={{ marginTop: 16, padding: 16, background: 'rgba(33, 150, 243, 0.1)', borderRadius: 4 }}>
        <div style={{ fontSize: 14, marginBottom: 8 }}>
          <strong>Connection Info</strong>
        </div>
        <div style={{ fontSize: 12, color: '#999' }}>
          <div>• URL: MynaDB HTTP API endpoint (e.g., http://localhost:7000)</div>
          <div>• Database: Catalog/database name for queries</div>
          <div>• Access: Use "Server (default)" for backend-to-backend communication</div>
        </div>
      </div>
    </FieldSet>
  );
}
