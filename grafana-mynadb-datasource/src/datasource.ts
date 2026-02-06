import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MutableDataFrame,
  FieldType,
  TimeRange,
  ScopedVars,
  MetricFindValue,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { MynaDBQuery, MynaDBDataSourceOptions, defaultQuery } from './types';

export class MynaDBDataSource extends DataSourceApi<MynaDBQuery, MynaDBDataSourceOptions> {
  url: string;
  database: string;

  constructor(instanceSettings: DataSourceInstanceSettings<MynaDBDataSourceOptions>) {
    super(instanceSettings);
    this.url = instanceSettings.url || '';
    this.database = instanceSettings.jsonData.database || 'default';
  }

  getDefaultQuery(): Partial<MynaDBQuery> {
    return defaultQuery;
  }

  async query(options: DataQueryRequest<MynaDBQuery>): Promise<DataQueryResponse> {
    const { range, maxDataPoints, interval, intervalMs } = options;

    // Build Grafana query request
    const request = {
      range: {
        from: range!.from.toISOString(),
        to: range!.to.toISOString(),
      },
      maxDataPoints,
      interval,
      intervalMs,
      targets: options.targets
        .filter((target) => !target.hide && target.rawSql)
        .map((target) => ({
          refId: target.refId,
          rawSql: this.interpolateVariables(target.rawSql || '', options.scopedVars, range!),
          format: target.format || 'time_series',
        })),
    };

    if (request.targets.length === 0) {
      return { data: [] };
    }

    try {
      // Send to MynaDB Grafana endpoint
      const response = await getBackendSrv().post(`${this.url}/grafana/query`, request);

      // Transform response to Grafana data frames
      return { data: this.transformResponse(response) };
    } catch (error) {
      console.error('MynaDB query error:', error);
      throw error;
    }
  }

  async testDatasource() {
    try {
      await getBackendSrv().get(`${this.url}/grafana/health`);
      return {
        status: 'success',
        message: 'Successfully connected to MynaDB',
      };
    } catch (error: any) {
      return {
        status: 'error',
        message: error.message || 'Cannot connect to MynaDB',
      };
    }
  }

  async metricFindQuery(query: string): Promise<MetricFindValue[]> {
    try {
      const response = await getBackendSrv().get(`${this.url}/grafana/search`, {
        target: query || this.database,
      });
      return response.map((item: any) => ({
        text: item.text,
        value: item.value,
      }));
    } catch (error) {
      console.error('Metric find query error:', error);
      return [];
    }
  }

  interpolateVariables(sql: string, scopedVars: ScopedVars, range: TimeRange): string {
    // Use Grafana's template service for standard variable replacement
    sql = getTemplateSrv().replace(sql, scopedVars);

    // Handle $__timeFilter(column) macro
    const timeFilterRegex = /\$__timeFilter\((\w+)\)/g;
    sql = sql.replace(timeFilterRegex, (match, column) => {
      const from = range.from.valueOf() * 1000000; // ms to ns
      const to = range.to.valueOf() * 1000000;
      return `${column} >= ${from} AND ${column} <= ${to}`;
    });

    // Handle $__interval macro
    const interval = this.calculateInterval(range);
    sql = sql.replace(/\$__interval/g, interval);

    // Handle $__from and $__to macros
    sql = sql.replace(/\$__from/g, (range.from.valueOf() * 1000000).toString());
    sql = sql.replace(/\$__to/g, (range.to.valueOf() * 1000000).toString());

    return sql;
  }

  calculateInterval(range: TimeRange): string {
    const diffMs = range.to.valueOf() - range.from.valueOf();
    const hours = diffMs / (1000 * 60 * 60);

    if (hours < 1) {
      return '1m';
    } else if (hours < 6) {
      return '5m';
    } else if (hours < 24) {
      return '15m';
    } else if (hours < 168) {
      // 1 week
      return '1h';
    } else {
      return '1d';
    }
  }

  transformResponse(response: any[]): MutableDataFrame[] {
    const frames: MutableDataFrame[] = [];

    for (const item of response) {
      if (item.datapoints) {
        // Time series format
        const frame = new MutableDataFrame({
          name: item.target,
          refId: item.target,
          fields: [
            { name: 'Time', type: FieldType.time },
            { name: 'Value', type: FieldType.number },
          ],
        });

        for (const point of item.datapoints) {
          const [value, time] = point;
          frame.add({ Time: time, Value: value });
        }

        frames.push(frame);
      } else if (item.columns && item.rows) {
        // Table format
        const frame = new MutableDataFrame({
          refId: item.refId,
          fields: item.columns.map((col: any) => ({
            name: col.text,
            type: this.mapGrafanaType(col.type),
          })),
        });

        for (const row of item.rows) {
          const rowData: any = {};
          item.columns.forEach((col: any, idx: number) => {
            rowData[col.text] = row[idx];
          });
          frame.add(rowData);
        }

        frames.push(frame);
      }
    }

    return frames;
  }

  mapGrafanaType(grafanaType: string): FieldType {
    switch (grafanaType) {
      case 'time':
        return FieldType.time;
      case 'number':
        return FieldType.number;
      case 'string':
        return FieldType.string;
      default:
        return FieldType.other;
    }
  }
}
