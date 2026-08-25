import { describe, expect, it } from 'vitest';
import { AGGREGATION_TEMPORALITY_DELTA, encodeOtlpMetricsData, metricNameMatches } from './otlp-metrics';

function readVarint(buf: Buffer, offset: number): { value: number; offset: number } {
  let value = 0;
  let shift = 0;
  let i = offset;
  while (i < buf.length) {
    const byte = buf[i]!;
    i += 1;
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return { value, offset: i };
    shift += 7;
  }
  throw new Error('truncated varint');
}

describe('instrumentation/otlp-metrics', () => {
  it('encodes MetricsData with a length-delimited resource_metrics field', () => {
    const encoded = encodeOtlpMetricsData({
      resourceAttributes: [{ key: 'service.name', value: 'demo' }],
      scopeName: 'kafka-core',
      scopeVersion: '1.0.0',
      metrics: [
        {
          name: 'kafka.producer.retry',
          unit: '{retry}',
          sum: {
            dataPoints: [
              {
                startTimeUnixNano: 1n,
                timeUnixNano: 2n,
                asInt: 3n,
              },
            ],
            aggregationTemporality: AGGREGATION_TEMPORALITY_DELTA,
            isMonotonic: true,
          },
        },
      ],
    });

    expect(encoded.length).toBeGreaterThan(16);
    const first = readVarint(encoded, 0);
    expect(first.value & 0x07).toBe(2);
    expect(first.value >> 3).toBe(1);
    const inner = readVarint(encoded, first.offset);
    expect(inner.value).toBe(encoded.length - inner.offset);
    expect(encoded.toString('utf8')).toContain('kafka.producer.retry');
    expect(encoded.toString('utf8')).toContain('service.name');
    expect(encoded.toString('utf8')).toContain('demo');
  });

  it('metricNameMatches follows KIP-714 prefix rules', () => {
    expect(metricNameMatches('kafka.client.request.duration', [])).toBe(false);
    expect(metricNameMatches('kafka.client.request.duration', [''])).toBe(true);
    expect(metricNameMatches('kafka.client.request.duration', ['kafka.client.'])).toBe(true);
    expect(metricNameMatches('kafka.producer.retry', ['kafka.client.'])).toBe(false);
    expect(metricNameMatches('kafka.producer.retry', ['kafka.producer.retry'])).toBe(true);
  });
});
