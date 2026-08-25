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
} from '../network/instrumentation-events';
import type { InstrumentationEventEmitter } from './emitter';
import { METRIC_NAMES, type ProduceMetrics } from './metrics';
import {
  AGGREGATION_TEMPORALITY_CUMULATIVE,
  AGGREGATION_TEMPORALITY_DELTA,
  encodeOtlpMetricsData,
  metricNameMatches,
  type OtlpKeyValue,
  type OtlpMetric,
} from './otlp-metrics';

interface CounterState {
  value: number;
}

interface HistogramState {
  count: number;
  sum: number;
}

function nowUnixNano(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

/**
 * In-process counters/histograms for KIP-714, keyed by {@link METRIC_NAMES}.
 * Independent of the optional OpenTelemetry meter.
 */
export class TelemetrySnapshot {
  readonly #counters = new Map<string, CounterState>();
  readonly #histograms = new Map<string, HistogramState>();
  readonly #upDown = new Map<string, CounterState>();
  #startTimeUnixNano = nowUnixNano();

  bind(emitter: InstrumentationEventEmitter): void {
    emitter.addListener(NETWORK_REQUEST, (event) => {
      const { duration, size } = event.payload as NetworkRequestEvent;
      this.#observe(METRIC_NAMES.requestDuration, duration);
      this.#observe(METRIC_NAMES.requestSize, size);
    });
    emitter.addListener(NETWORK_REQUEST_TIMEOUT, () => {
      this.#add(METRIC_NAMES.requestTimeout, 1);
    });
    emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, (event) => {
      const payload = event.payload as NetworkRequestQueueSizeEvent;
      this.#observe(METRIC_NAMES.requestQueueSize, payload.queueSize);
    });

    emitter.addListener(PRODUCER_CONNECT, () => this.#addUpDown(METRIC_NAMES.connectionCount, 1));
    emitter.addListener(PRODUCER_DISCONNECT, () => this.#addUpDown(METRIC_NAMES.connectionCount, -1));
    emitter.addListener(CONSUMER_CONNECT, () => this.#addUpDown(METRIC_NAMES.connectionCount, 1));
    emitter.addListener(CONSUMER_DISCONNECT, () => this.#addUpDown(METRIC_NAMES.connectionCount, -1));
    emitter.addListener(ADMIN_CONNECT, () => this.#addUpDown(METRIC_NAMES.connectionCount, 1));
    emitter.addListener(ADMIN_DISCONNECT, () => this.#addUpDown(METRIC_NAMES.connectionCount, -1));

    emitter.addListener(CONSUMER_FETCH, (event) => {
      const payload = event.payload as FetchPayload;
      this.#observe(METRIC_NAMES.consumerFetchDuration, payload.duration);
    });
    emitter.addListener(SHARE_FETCH, (event) => {
      const payload = event.payload as ShareFetchPayload;
      this.#observe(METRIC_NAMES.consumerFetchDuration, payload.duration);
    });
    emitter.addListener(END_BATCH_PROCESS, (event) => {
      const { batchSize, offsetLag } = event.payload as BatchProcessPayload;
      this.#add(METRIC_NAMES.consumerFetchRecords, batchSize);
      this.#observe(METRIC_NAMES.consumerLag, Number(offsetLag));
    });
    emitter.addListener(REBALANCING, () => this.#add(METRIC_NAMES.consumerRebalance, 1));
    emitter.addListener(GROUP_JOIN, () => this.#add(METRIC_NAMES.consumerGroupJoin, 1));
  }

  recordProduce({ records, bytes, retries }: ProduceMetrics): void {
    if (records > 0) {
      this.#add(METRIC_NAMES.producerRecordSend, records);
      this.#observe(METRIC_NAMES.producerRecordSize, bytes / records);
      this.#observe(METRIC_NAMES.producerBatchSize, records);
    }
    if (retries > 0) this.#add(METRIC_NAMES.producerRetry, retries);
  }

  encode(options: {
    requestedMetrics: readonly string[];
    deltaTemporality: boolean;
    resourceAttributes: OtlpKeyValue[];
    scopeName: string;
    scopeVersion: string;
  }): Buffer {
    const time = nowUnixNano();
    const temporality = options.deltaTemporality ? AGGREGATION_TEMPORALITY_DELTA : AGGREGATION_TEMPORALITY_CUMULATIVE;
    const metrics: OtlpMetric[] = [];

    for (const [name, state] of this.#counters) {
      if (!metricNameMatches(name, options.requestedMetrics)) continue;
      const value = state.value;
      if (options.deltaTemporality) {
        state.value = 0;
        if (value === 0) continue;
      }
      metrics.push({
        name,
        unit: '{1}',
        sum: {
          dataPoints: [{ startTimeUnixNano: this.#startTimeUnixNano, timeUnixNano: time, asInt: BigInt(value) }],
          aggregationTemporality: temporality,
          isMonotonic: true,
        },
      });
    }

    for (const [name, state] of this.#upDown) {
      if (!metricNameMatches(name, options.requestedMetrics)) continue;
      metrics.push({
        name,
        unit: '{connection}',
        sum: {
          dataPoints: [{ startTimeUnixNano: this.#startTimeUnixNano, timeUnixNano: time, asInt: BigInt(state.value) }],
          aggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
          isMonotonic: false,
        },
      });
    }

    for (const [name, state] of this.#histograms) {
      if (!metricNameMatches(name, options.requestedMetrics)) continue;
      const count = state.count;
      const sum = state.sum;
      if (options.deltaTemporality) {
        state.count = 0;
        state.sum = 0;
        if (count === 0) continue;
      }
      metrics.push({
        name,
        unit: '1',
        histogram: {
          dataPoints: [
            {
              startTimeUnixNano: this.#startTimeUnixNano,
              timeUnixNano: time,
              count: BigInt(count),
              sum,
            },
          ],
          aggregationTemporality: temporality,
        },
      });
    }

    if (options.deltaTemporality) this.#startTimeUnixNano = time;

    return encodeOtlpMetricsData({
      resourceAttributes: options.resourceAttributes,
      scopeName: options.scopeName,
      scopeVersion: options.scopeVersion,
      metrics,
    });
  }

  #add(name: string, amount: number): void {
    const current = this.#counters.get(name) ?? { value: 0 };
    current.value += amount;
    this.#counters.set(name, current);
  }

  #addUpDown(name: string, amount: number): void {
    const current = this.#upDown.get(name) ?? { value: 0 };
    current.value += amount;
    this.#upDown.set(name, current);
  }

  #observe(name: string, value: number): void {
    const current = this.#histograms.get(name) ?? { count: 0, sum: 0 };
    current.count += 1;
    current.sum += value;
    this.#histograms.set(name, current);
  }
}
