/**
 * Minimal encoder for OTLP MetricsData v1 protobuf (KIP-714 PushTelemetry payload).
 * Only the fields this client emits are implemented — not a general protobuf library.
 *
 * @see https://github.com/open-telemetry/opentelemetry-proto/blob/v1.5.0/opentelemetry/proto/metrics/v1/metrics.proto
 */

const WIRE_VARINT = 0;
const WIRE_64BIT = 1;
const WIRE_LEN = 2;

export const AGGREGATION_TEMPORALITY_DELTA = 1;
export const AGGREGATION_TEMPORALITY_CUMULATIVE = 2;

export interface OtlpKeyValue {
  key: string;
  value: string;
}

export interface OtlpNumberPoint {
  attributes?: OtlpKeyValue[];
  startTimeUnixNano: bigint;
  timeUnixNano: bigint;
  asInt: bigint;
}

export interface OtlpHistogramPoint {
  attributes?: OtlpKeyValue[];
  startTimeUnixNano: bigint;
  timeUnixNano: bigint;
  count: bigint;
  sum: number;
}

export type OtlpMetric =
  | {
      name: string;
      description?: string;
      unit?: string;
      sum: {
        dataPoints: OtlpNumberPoint[];
        aggregationTemporality: number;
        isMonotonic: boolean;
      };
    }
  | {
      name: string;
      description?: string;
      unit?: string;
      histogram: {
        dataPoints: OtlpHistogramPoint[];
        aggregationTemporality: number;
      };
    };

export interface OtlpMetricsData {
  resourceAttributes: OtlpKeyValue[];
  scopeName: string;
  scopeVersion: string;
  metrics: OtlpMetric[];
}

function varint(value: number | bigint): Buffer {
  let n = typeof value === 'bigint' ? value : BigInt(value);
  if (n < 0n) throw new RangeError('protobuf varint must be non-negative');
  const bytes: number[] = [];
  while (n >= 0x80n) {
    bytes.push(Number(n & 0x7fn) | 0x80);
    n >>= 7n;
  }
  bytes.push(Number(n));
  return Buffer.from(bytes);
}

function tag(field: number, wire: number): Buffer {
  return varint((field << 3) | wire);
}

function lenField(field: number, payload: Buffer): Buffer {
  return Buffer.concat([tag(field, WIRE_LEN), varint(payload.length), payload]);
}

function varintField(field: number, value: number | bigint): Buffer {
  return Buffer.concat([tag(field, WIRE_VARINT), varint(value)]);
}

function fixed64Field(field: number, value: bigint): Buffer {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(value);
  return Buffer.concat([tag(field, WIRE_64BIT), buf]);
}

function sfixed64Field(field: number, value: bigint): Buffer {
  const buf = Buffer.alloc(8);
  buf.writeBigInt64LE(value);
  return Buffer.concat([tag(field, WIRE_64BIT), buf]);
}

function doubleField(field: number, value: number): Buffer {
  const buf = Buffer.alloc(8);
  buf.writeDoubleLE(value);
  return Buffer.concat([tag(field, WIRE_64BIT), buf]);
}

function stringField(field: number, value: string): Buffer {
  return lenField(field, Buffer.from(value, 'utf8'));
}

function boolField(field: number, value: boolean): Buffer {
  return varintField(field, value ? 1 : 0);
}

function encodeAnyValueString(value: string): Buffer {
  return stringField(1, value);
}

function encodeKeyValue({ key, value }: OtlpKeyValue): Buffer {
  return Buffer.concat([stringField(1, key), lenField(2, encodeAnyValueString(value))]);
}

function encodeNumberPoint(point: OtlpNumberPoint): Buffer {
  const parts: Buffer[] = [
    fixed64Field(2, point.startTimeUnixNano),
    fixed64Field(3, point.timeUnixNano),
    sfixed64Field(6, point.asInt),
  ];
  for (const attribute of point.attributes ?? []) {
    parts.push(lenField(7, encodeKeyValue(attribute)));
  }
  return Buffer.concat(parts);
}

function encodeHistogramPoint(point: OtlpHistogramPoint): Buffer {
  const parts: Buffer[] = [
    fixed64Field(2, point.startTimeUnixNano),
    fixed64Field(3, point.timeUnixNano),
    varintField(4, point.count),
    doubleField(5, point.sum),
  ];
  for (const attribute of point.attributes ?? []) {
    parts.push(lenField(9, encodeKeyValue(attribute)));
  }
  return Buffer.concat(parts);
}

function encodeMetric(metric: OtlpMetric): Buffer {
  const parts: Buffer[] = [stringField(1, metric.name)];
  if (metric.description) parts.push(stringField(2, metric.description));
  if (metric.unit) parts.push(stringField(3, metric.unit));

  if ('sum' in metric) {
    const sumParts: Buffer[] = [];
    for (const point of metric.sum.dataPoints) {
      sumParts.push(lenField(1, encodeNumberPoint(point)));
    }
    sumParts.push(varintField(2, metric.sum.aggregationTemporality));
    sumParts.push(boolField(3, metric.sum.isMonotonic));
    parts.push(lenField(7, Buffer.concat(sumParts)));
  } else {
    const histParts: Buffer[] = [];
    for (const point of metric.histogram.dataPoints) {
      histParts.push(lenField(1, encodeHistogramPoint(point)));
    }
    histParts.push(varintField(2, metric.histogram.aggregationTemporality));
    parts.push(lenField(9, Buffer.concat(histParts)));
  }

  return Buffer.concat(parts);
}

function encodeInstrumentationScope(name: string, version: string): Buffer {
  return Buffer.concat([stringField(1, name), stringField(2, version)]);
}

function encodeResource(attributes: OtlpKeyValue[]): Buffer {
  return Buffer.concat(attributes.map((attribute) => lenField(1, encodeKeyValue(attribute))));
}

function encodeScopeMetrics(data: OtlpMetricsData): Buffer {
  const parts: Buffer[] = [lenField(1, encodeInstrumentationScope(data.scopeName, data.scopeVersion))];
  for (const metric of data.metrics) {
    parts.push(lenField(2, encodeMetric(metric)));
  }
  return Buffer.concat(parts);
}

function encodeResourceMetrics(data: OtlpMetricsData): Buffer {
  return Buffer.concat([lenField(1, encodeResource(data.resourceAttributes)), lenField(2, encodeScopeMetrics(data))]);
}

/** Encode a MetricsData protobuf with one ResourceMetrics / ScopeMetrics. */
export function encodeOtlpMetricsData(data: OtlpMetricsData): Buffer {
  return lenField(1, encodeResourceMetrics(data));
}

/** Match KIP-714 requested-metrics prefixes. Empty list = none; `[""]` = all. */
export function metricNameMatches(name: string, requested: readonly string[]): boolean {
  if (requested.length === 0) return false;
  if (requested.length === 1 && requested[0] === '') return true;
  return requested.some((prefix) => prefix.length > 0 && name.startsWith(prefix));
}
