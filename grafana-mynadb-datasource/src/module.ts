import { DataSourcePlugin } from '@grafana/data';
import { MynaDBDataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { MynaDBQuery, MynaDBDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<MynaDBDataSource, MynaDBQuery, MynaDBDataSourceOptions>(
  MynaDBDataSource
)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
