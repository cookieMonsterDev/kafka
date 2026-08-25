import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../errors';
import { InstrumentationEventEmitter } from './emitter';
import {
  CONNECT as CONSUMER_CONNECT,
  DISCONNECT as CONSUMER_DISCONNECT,
  END_BATCH_PROCESS,
  FETCH as CONSUMER_FETCH,
  GROUP_JOIN,
  REBALANCING,
} from '../consumer/instrumentation-events';
import { CONNECT as PRODUCER_CONNECT, DISCONNECT as PRODUCER_DISCONNECT } from '../producer/instrumentation-events';
import {
  NETWORK_REQUEST,
  NETWORK_REQUEST_QUEUE_SIZE,
  NETWORK_REQUEST_TIMEOUT,
} from '../network/instrumentation-events';
import {
  createMetricsRecorder,
  METRIC_NAMES,
  MetricsRecorder,
  type KafkaCounter,
  type KafkaHistogram,
  type KafkaMeter,
  type KafkaMetricAttributes,
  type KafkaUpDownCounter,
} from './metrics';

class MemoryInstrument implements KafkaCounter, KafkaHistogram, KafkaUpDownCounter {
  readonly points: { value: number; attributes?: KafkaMetricAttributes }[] = [];

  add(value: number, attributes?: KafkaMetricAttributes): void {
    this.points.push({ value, attributes });
  }

  record(value: number, attributes?: KafkaMetricAttributes): void {
    this.points.push({ value, attributes });
  }
}

function memoryMeter(): { meter: KafkaMeter; instruments: Map<string, MemoryInstrument> } {
  const instruments = new Map<string, MemoryInstrument>();
  const meter: KafkaMeter = {
    createCounter(name) {
      const instrument = new MemoryInstrument();
      instruments.set(name, instrument);
      return instrument;
    },
    createHistogram(name) {
      const instrument = new MemoryInstrument();
      instruments.set(name, instrument);
      return instrument;
    },
    createUpDownCounter(name) {
      const instrument = new MemoryInstrument();
      instruments.set(name, instrument);
      return instrument;
    },
  };
  return { meter, instruments };
}

describe('instrumentation/metrics', () => {
  it('createMetricsRecorder is off by default and for false', () => {
    expect(createMetricsRecorder(undefined)).toBeNull();
    expect(createMetricsRecorder(false)).toBeNull();
  });

  it('createMetricsRecorder(true) throws when the OpenTelemetry meter cannot be loaded', () => {
    expect(() => createMetricsRecorder(true, () => null)).toThrow(KafkaNonRetriableError);
    expect(() => createMetricsRecorder(true, () => null)).toThrow(/@opentelemetry\/api/);
  });

  it('createMetricsRecorder(true) uses a loaded global meter', () => {
    const { meter } = memoryMeter();
    expect(createMetricsRecorder(true, () => meter)).toBeInstanceOf(MetricsRecorder);
  });

  it('createMetricsRecorder({ meter }) records produce and request metrics', () => {
    const { meter, instruments } = memoryMeter();
    const recorder = createMetricsRecorder({ meter });
    expect(recorder).toBeInstanceOf(MetricsRecorder);

    const emitter = new InstrumentationEventEmitter();
    recorder!.bind(emitter, 'producer');

    emitter.emit(PRODUCER_CONNECT, {});
    emitter.emit(NETWORK_REQUEST, {
      broker: 'localhost:9092',
      clientId: 'c',
      correlationId: 1,
      size: 128,
      createdAt: 0,
      sentAt: 1,
      pendingDuration: 1,
      duration: 12,
      apiName: 'Produce',
      apiKey: 0,
      apiVersion: 9,
    });
    emitter.emit(NETWORK_REQUEST_TIMEOUT, {
      broker: 'localhost:9092',
      clientId: 'c',
      correlationId: 2,
      createdAt: 0,
      sentAt: 1,
      pendingDuration: 1,
      apiName: 'Produce',
      apiKey: 0,
      apiVersion: 9,
    });
    emitter.emit(NETWORK_REQUEST_QUEUE_SIZE, { broker: 'localhost:9092', clientId: 'c', queueSize: 3 });
    recorder!.recordProduce({ records: 4, bytes: 40, retries: 2 });
    emitter.emit(PRODUCER_DISCONNECT, {});

    expect(instruments.get(METRIC_NAMES.connectionCount)?.points).toEqual([
      { value: 1, attributes: { client: 'producer' } },
      { value: -1, attributes: { client: 'producer' } },
    ]);
    expect(instruments.get(METRIC_NAMES.requestDuration)?.points).toEqual([
      { value: 12, attributes: { client: 'producer', api_name: 'Produce', broker: 'localhost:9092' } },
    ]);
    expect(instruments.get(METRIC_NAMES.requestSize)?.points).toEqual([
      { value: 128, attributes: { client: 'producer', api_name: 'Produce', broker: 'localhost:9092' } },
    ]);
    expect(instruments.get(METRIC_NAMES.requestTimeout)?.points).toEqual([
      { value: 1, attributes: { client: 'producer', api_name: 'Produce', broker: 'localhost:9092' } },
    ]);
    expect(instruments.get(METRIC_NAMES.requestQueueSize)?.points).toEqual([
      { value: 3, attributes: { client: 'producer', broker: 'localhost:9092' } },
    ]);
    expect(instruments.get(METRIC_NAMES.producerRecordSend)?.points).toEqual([{ value: 4, attributes: undefined }]);
    expect(instruments.get(METRIC_NAMES.producerRecordSize)?.points).toEqual([{ value: 10, attributes: undefined }]);
    expect(instruments.get(METRIC_NAMES.producerBatchSize)?.points).toEqual([{ value: 4, attributes: undefined }]);
    expect(instruments.get(METRIC_NAMES.producerRetry)?.points).toEqual([{ value: 2, attributes: undefined }]);
  });

  it('records consumer fetch, lag, rebalance, and group join', () => {
    const { meter, instruments } = memoryMeter();
    const recorder = new MetricsRecorder(meter);
    const emitter = new InstrumentationEventEmitter();
    recorder.bind(emitter, 'consumer');

    emitter.emit(CONSUMER_CONNECT, {});
    emitter.emit(CONSUMER_FETCH, { numberOfBatches: 1, duration: 8, nodeId: '1' });
    emitter.emit(END_BATCH_PROCESS, {
      topic: 'events',
      partition: 0,
      highWatermark: 10n,
      offsetLag: 3n,
      offsetLagLow: 0n,
      batchSize: 5,
      firstOffset: 7n,
      lastOffset: 9n,
      duration: 1,
    });
    emitter.emit(REBALANCING, { groupId: 'g', memberId: 'm' });
    emitter.emit(GROUP_JOIN, {
      duration: 1,
      groupId: 'g',
      isLeader: true,
      leaderId: 'm',
      groupProtocol: 'range',
      memberId: 'm',
      memberAssignment: {},
    });
    emitter.emit(CONSUMER_DISCONNECT, {});

    expect(instruments.get(METRIC_NAMES.consumerFetchDuration)?.points).toEqual([
      { value: 8, attributes: { client: 'consumer', node_id: '1' } },
    ]);
    expect(instruments.get(METRIC_NAMES.consumerFetchRecords)?.points).toEqual([
      { value: 5, attributes: { client: 'consumer', topic: 'events', partition: 0 } },
    ]);
    expect(instruments.get(METRIC_NAMES.consumerLag)?.points).toEqual([
      { value: 3, attributes: { client: 'consumer', topic: 'events', partition: 0 } },
    ]);
    expect(instruments.get(METRIC_NAMES.consumerRebalance)?.points).toEqual([
      { value: 1, attributes: { client: 'consumer' } },
    ]);
    expect(instruments.get(METRIC_NAMES.consumerGroupJoin)?.points).toEqual([
      { value: 1, attributes: { client: 'consumer' } },
    ]);
  });

  it('recordProduce is a no-op for empty sends', () => {
    const { meter, instruments } = memoryMeter();
    const recorder = new MetricsRecorder(meter);
    recorder.recordProduce({ records: 0, bytes: 0, retries: 0 });
    expect(instruments.get(METRIC_NAMES.producerRecordSend)?.points).toEqual([]);
    expect(instruments.get(METRIC_NAMES.producerRetry)?.points).toEqual([]);
  });
});
