import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface MynaDBQuery extends DataQuery {
  rawSql?: string;
  queryMode?: 'sql' | 'influxql' | 'builder';
  format?: 'time_series' | 'table';
}

export const defaultQuery: Partial<MynaDBQuery> = {
  rawSql: 'SELECT time, value FROM metrics WHERE $__timeFilter(time)',
  queryMode: 'sql',
  format: 'time_series',
};

export interface MynaDBDataSourceOptions extends DataSourceJsonData {
  database?: string;
}

export interface MynaDBSecureJsonData {
  apiKey?: string;
}
