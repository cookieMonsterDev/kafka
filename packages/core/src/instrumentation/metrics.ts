import { createRequire } from 'node:module';
import { KafkaNonRetriableError } from '../errors';
import {
  END_BATCH_PROCESS,
  GROUP_JOIN,
  REBALANCING,
  FETCH as CONSUMER_FETCH,
  CONNECT as CONSUMER_CONNECT,
  DISCONNECT as CONSUMER_DISCONNECT,
} from '../consumer/instrumentation-events';
import type { BatchProcessPayload, FetchPayload } from '../consumer/instrumentation-events';
import { CONNECT as PRODUCER_CONNECT, DISCONNECT as PRODUCER_DISCONNECT } from '../producer/instrumentation-events';
import { CONNECT as ADMIN_CONNECT, DISCONNECT as ADMIN_DISCONNECT } from '../admin/instrumentation-events';
import { FETCH as SHARE_FETCH } from '../share-consumer/instrumentation-events';
import type { ShareFetchPayload } from '../share-consumer/instrumentation-events';
import {
  NETWORK_REQUEST,
  NETWORK_REQUEST_QUEUE_SIZE,
  NETWORK_REQUEST_TIMEOUT,
  type NetworkRequestEvent,
  type NetworkRequestQueueSizeEvent,
  type NetworkRequestTimeoutEvent,
} from '../network/instrumentation-events';
import type { InstrumentationEventEmitter } from './emitter';

/** Attribute values accepted by the OpenTelemetry metrics API. */
export type KafkaMetricAttributeValue = string | number | boolean;

export type KafkaMetricAttributes = Record<string, KafkaMetricAttributeValue | undefined>;

export interface KafkaCounter {
  add(value: number, attributes?: KafkaMetricAttributes): void;
}

export interface KafkaHistogram {
  record(value: number, attributes?: KafkaMetricAttributes): void;
}

export interface KafkaUpDownCounter {
  add(value: number, attributes?: KafkaMetricAttributes): void;
}

/**
 * Subset of the OpenTelemetry `Meter` API. An `@opentelemetry/api` `Meter` is
 * structurally assignable and can be passed as {@link KafkaMetricsConfig.meter}.
 */
export interface KafkaMeter {
  createCounter(name: string, options?: { description?: string; unit?: string }): KafkaCounter;
  createHistogram(name: string, options?: { description?: string; unit?: string }): KafkaHistogram;
  createUpDownCounter(name: string, options?: { description?: string; unit?: string }): KafkaUpDownCounter;
}

/** `true` loads the global OpenTelemetry meter; `{ meter }` supplies one directly. Off when omitted. */
export type KafkaMetrics = boolean | KafkaMetricsConfig;

export interface KafkaMetricsConfig {
  meter: KafkaMeter;
}

export type MetricsClient = 'producer' | 'consumer' | 'admin' | 'share_consumer';

/** Stable metric names so KIP-714 can reuse them. */
export const METRIC_NAMES = Object.freeze({
  connectionCount: 'kafka.client.connection.count',
  requestDuration: 'kafka.client.request.duration',
  requestSize: 'kafka.client.request.size',
  requestTimeout: 'kafka.client.request.timeout',
  requestQueueSize: 'kafka.client.request.queue_size',
  producerRecordSend: 'kafka.producer.record.send',
  producerRecordSize: 'kafka.producer.record.size',
  producerBatchSize: 'kafka.producer.batch.size',
  producerRetry: 'kafka.producer.retry',
  consumerFetchRecords: 'kafka.consumer.fetch.records',
  consumerFetchDuration: 'kafka.consumer.fetch.duration',
  consumerLag: 'kafka.consumer.lag',
  consumerRebalance: 'kafka.consumer.rebalance',
  consumerGroupJoin: 'kafka.consumer.group_join',
});

export interface ProduceMetrics {
  records: number;
  bytes: number;
  retries: number;
}

export class MetricsRecorder {
  readonly #connectionCount: KafkaUpDownCounter;
  readonly #requestDuration: KafkaHistogram;
  readonly #requestSize: KafkaHistogram;
  readonly #requestTimeout: KafkaCounter;
  readonly #requestQueueSize: KafkaHistogram;
  readonly #producerRecordSend: KafkaCounter;
  readonly #producerRecordSize: KafkaHistogram;
  readonly #producerBatchSize: KafkaHistogram;
  readonly #producerRetry: KafkaCounter;
  readonly #consumerFetchRecords: KafkaCounter;
  readonly #consumerFetchDuration: KafkaHistogram;
  readonly #consumerLag: KafkaHistogram;
  readonly #consumerRebalance: KafkaCounter;
  readonly #consumerGroupJoin: KafkaCounter;

  constructor(meter: KafkaMeter) {
    this.#connectionCount = meter.createUpDownCounter(METRIC_NAMES.connectionCount, {
      description: 'Open producer, consumer, and admin clients',
      unit: '{connection}',
    });
    this.#requestDuration = meter.createHistogram(METRIC_NAMES.requestDuration, {
      description: 'Broker RPC round-trip time',
      unit: 'ms',
    });
    this.#requestSize = meter.createHistogram(METRIC_NAMES.requestSize, {
      description: 'Broker RPC request size',
      unit: 'By',
    });
    this.#requestTimeout = meter.createCounter(METRIC_NAMES.requestTimeout, {
      description: 'Broker RPCs that timed out on the client',
      unit: '{timeout}',
    });
    this.#requestQueueSize = meter.createHistogram(METRIC_NAMES.requestQueueSize, {
      description: 'In-flight requests queued on a broker connection',
      unit: '{request}',
    });
    this.#producerRecordSend = meter.createCounter(METRIC_NAMES.producerRecordSend, {
      description: 'Records accepted by a successful Produce',
      unit: '{record}',
    });
    this.#producerRecordSize = meter.createHistogram(METRIC_NAMES.producerRecordSize, {
      description: 'Uncompressed key+value bytes per produced record',
      unit: 'By',
    });
    this.#producerBatchSize = meter.createHistogram(METRIC_NAMES.producerBatchSize, {
      description: 'Records in one send()/sendBatch() Produce attempt',
      unit: '{record}',
    });
    this.#producerRetry = meter.createCounter(METRIC_NAMES.producerRetry, {
      description: 'Produce attempts after the first try',
      unit: '{retry}',
    });
    this.#consumerFetchRecords = meter.createCounter(METRIC_NAMES.consumerFetchRecords, {
      description: 'Records delivered to a consume handler from one fetch batch',
      unit: '{record}',
    });
    this.#consumerFetchDuration = meter.createHistogram(METRIC_NAMES.consumerFetchDuration, {
      description: 'Time spent waiting on Fetch / ShareFetch',
      unit: 'ms',
    });
    this.#consumerLag = meter.createHistogram(METRIC_NAMES.consumerLag, {
      description: 'High-watermark lag observed on a fetched batch',
      unit: '{offset}',
    });
    this.#consumerRebalance = meter.createCounter(METRIC_NAMES.consumerRebalance, {
      description: 'Consumer group rebalances started',
      unit: '{rebalance}',
    });
    this.#consumerGroupJoin = meter.createCounter(METRIC_NAMES.consumerGroupJoin, {
      description: 'Successful consumer group joins',
      unit: '{join}',
    });
  }

  bind(emitter: InstrumentationEventEmitter, client: MetricsClient): void {
    const clientAttrs = { client };

    emitter.addListener(NETWORK_REQUEST, (event) => {
      const { duration, size, apiName, broker } = event.payload as NetworkRequestEvent;
      const attributes = { ...clientAttrs, api_name: apiName, broker };
      this.#requestDuration.record(duration, attributes);
      this.#requestSize.record(size, attributes);
    });

    emitter.addListener(NETWORK_REQUEST_TIMEOUT, (event) => {
      const payload = event.payload as NetworkRequestTimeoutEvent;
      this.#requestTimeout.add(1, { ...clientAttrs, api_name: payload.apiName, broker: payload.broker });
    });

    emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, (event) => {
      const payload = event.payload as NetworkRequestQueueSizeEvent;
      this.#requestQueueSize.record(payload.queueSize, { ...clientAttrs, broker: payload.broker });
    });

    if (client === 'producer') {
      emitter.addListener(PRODUCER_CONNECT, () => {
        this.#connectionCount.add(1, clientAttrs);
      });
      emitter.addListener(PRODUCER_DISCONNECT, () => {
        this.#connectionCount.add(-1, clientAttrs);
      });
    }

    if (client === 'consumer') {
      emitter.addListener(CONSUMER_CONNECT, () => {
        this.#connectionCount.add(1, clientAttrs);
      });
      emitter.addListener(CONSUMER_DISCONNECT, () => {
        this.#connectionCount.add(-1, clientAttrs);
      });
      emitter.addListener(CONSUMER_FETCH, (event) => {
        const payload = event.payload as FetchPayload;
        this.#consumerFetchDuration.record(payload.duration, { ...clientAttrs, node_id: payload.nodeId });
      });
      emitter.addListener(END_BATCH_PROCESS, (event) => {
        const { batchSize, offsetLag, topic, partition } = event.payload as BatchProcessPayload;
        const attributes = { ...clientAttrs, topic, partition };
        this.#consumerFetchRecords.add(batchSize, attributes);
        this.#consumerLag.record(Number(offsetLag), attributes);
      });
      emitter.addListener(REBALANCING, () => {
        this.#consumerRebalance.add(1, clientAttrs);
      });
      emitter.addListener(GROUP_JOIN, () => {
        this.#consumerGroupJoin.add(1, clientAttrs);
      });
    }

    if (client === 'admin') {
      emitter.addListener(ADMIN_CONNECT, () => {
        this.#connectionCount.add(1, clientAttrs);
      });
      emitter.addListener(ADMIN_DISCONNECT, () => {
        this.#connectionCount.add(-1, clientAttrs);
      });
    }

    if (client === 'share_consumer') {
      emitter.addListener(SHARE_FETCH, (event) => {
        const payload = event.payload as ShareFetchPayload;
        this.#consumerFetchDuration.record(payload.duration, { ...clientAttrs, node_id: payload.nodeId });
      });
    }
  }

  recordProduce({ records, bytes, retries }: ProduceMetrics): void {
    if (records > 0) {
      this.#producerRecordSend.add(records);
      this.#producerRecordSize.record(bytes / records);
      this.#producerBatchSize.record(records);
    }
    if (retries > 0) {
      this.#producerRetry.add(retries);
    }
  }
}

function tryLoadOpenTelemetryMeter(): KafkaMeter | null {
  try {
    const require = createRequire(import.meta.url);
    const otel = require('@opentelemetry/api') as {
      metrics: { getMeter: (name: string) => KafkaMeter };
    };
    return otel.metrics.getMeter('@cookiemonsterdev/kafka-core');
  } catch {
    return null;
  }
}

/** `undefined` / `false` → no recorder. `true` loads `@opentelemetry/api`. `{ meter }` uses that meter. */
export function createMetricsRecorder(
  config: KafkaMetrics | undefined,
  loadMeter: () => KafkaMeter | null = tryLoadOpenTelemetryMeter,
): MetricsRecorder | null {
  if (config === true) {
    const meter = loadMeter();
    if (meter == null) {
      throw new KafkaNonRetriableError(
        'metrics: true requires the optional peer dependency @opentelemetry/api; pass metrics: { meter } to supply a meter directly',
      );
    }
    return new MetricsRecorder(meter);
  }

  if (config != null && typeof config === 'object' && config.meter != null) {
    return new MetricsRecorder(config.meter);
  }

  return null;
}
