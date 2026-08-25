import { describe, expect, it } from 'vitest';
import { InstrumentationEventEmitter } from './emitter';
import { METRIC_NAMES } from './metrics';
import { NETWORK_REQUEST } from '../network/instrumentation-events';
import { TelemetrySnapshot } from './telemetry-snapshot';

describe('instrumentation/telemetry-snapshot', () => {
  it('encodes matching counters as OTLP and resets them in delta mode', () => {
    const snapshot = new TelemetrySnapshot();
    snapshot.recordProduce({ records: 4, bytes: 40, retries: 2 });

    const first = snapshot.encode({
      requestedMetrics: [''],
      deltaTemporality: true,
      resourceAttributes: [{ key: 'service.name', value: 't' }],
      scopeName: 'kafka-core',
      scopeVersion: '1',
    });
    expect(first.toString('utf8')).toContain(METRIC_NAMES.producerRecordSend);
    expect(first.toString('utf8')).toContain(METRIC_NAMES.producerRetry);

    const second = snapshot.encode({
      requestedMetrics: [METRIC_NAMES.producerRetry],
      deltaTemporality: true,
      resourceAttributes: [],
      scopeName: 'kafka-core',
      scopeVersion: '1',
    });
    expect(second.toString('utf8')).not.toContain(METRIC_NAMES.producerRetry);
  });

  it('records request histograms from the instrumentation emitter', () => {
    const emitter = new InstrumentationEventEmitter();
    const snapshot = new TelemetrySnapshot();
    snapshot.bind(emitter);
    emitter.emit(NETWORK_REQUEST, {
      duration: 12,
      size: 100,
      apiName: 'Produce',
      broker: 'localhost:9092',
    });

    const encoded = snapshot.encode({
      requestedMetrics: ['kafka.client.request.'],
      deltaTemporality: false,
      resourceAttributes: [],
      scopeName: 'kafka-core',
      scopeVersion: '1',
    });
    expect(encoded.toString('utf8')).toContain(METRIC_NAMES.requestDuration);
    expect(encoded.toString('utf8')).toContain(METRIC_NAMES.requestSize);
  });
});
