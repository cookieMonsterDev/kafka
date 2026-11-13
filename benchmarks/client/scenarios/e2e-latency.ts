import { performance } from 'node:perf_hooks';
import type { BenchRecord } from '../adapters/types.ts';
import { LatencyRecorder, type MeasurementWindow, startMeasurement } from '../measure.ts';
import type { RunMetrics } from '../spec.ts';
import { createSignal, pooledRecord, type ScenarioContext, type ScenarioRunner } from './harness.ts';

const stampBytes = 8;
// Every outgoing value is copied up front so stamping never races a send still in flight; this cap
// keeps that copy well inside the client's memory limit.
const maxPreallocatedValueBytes = 256 * 1024 * 1024;

function buildOutgoingSends(records: readonly BenchRecord[], count: number): BenchRecord[][] {
  const sends: BenchRecord[][] = [];
  let valueBytes = 0;
  for (let index = 0; index < count; index++) {
    const pooled = pooledRecord(records, index);
    if (pooled.value.length < stampBytes) {
      throw new Error(`E1 needs values of at least ${stampBytes} bytes to carry the send time`);
    }
    valueBytes += pooled.value.length;
    if (valueBytes > maxPreallocatedValueBytes) {
      throw new Error(
        `E1 would preallocate more than ${maxPreallocatedValueBytes} value bytes; lower rate or duration`,
      );
    }
    const value = Buffer.from(pooled.value);
    sends.push([pooled.headers === undefined ? { key: pooled.key, value } : { ...pooled, value }]);
  }
  return sends;
}

/**
 * E1: an open-loop producer sends at `rateMessagesPerSecond` while a consumer in the same process
 * reads the topic. Each value carries its intended send time (Float64 `performance.now()`) in its
 * first 8 bytes, and latency is receive time minus intended time, so a client that falls behind
 * schedule cannot hide it (coordinated omission). The first `warmupCount` records are warm-up;
 * `durationMs` worth of records at the rate form the measured window, which closes when the last
 * record arrives.
 */
export const runE2eLatencyScenario: ScenarioRunner = async (context: ScenarioContext): Promise<RunMetrics> => {
  const { spec, definition, observed } = context;
  const intervalMs = 1000 / spec.rateMessagesPerSecond;
  const measuredCount = Math.floor((spec.rateMessagesPerSecond * spec.durationMs) / 1000);
  const totalCount = spec.warmupCount + measuredCount;
  const outgoing = buildOutgoingSends(context.records, totalCount);
  let measuredValueBytes = 0;
  for (let index = spec.warmupCount; index < totalCount; index++) {
    measuredValueBytes += outgoing[index]?.[0]?.value.length ?? 0;
  }

  const latency = new LatencyRecorder(measuredCount);
  const allArrived = createSignal<void>();
  const allAcked = createSignal<void>();
  let measuredFromMs = Number.POSITIVE_INFINITY;

  const producer = await context.clients.connectProducer(context.adapter, context.adapterOptions);
  const consumer = await context.clients.connectConsumer(context.adapter, context.adapterOptions);
  const onDelivered = (): void => {
    if (observed.consumedRecords >= totalCount) {
      allArrived.resolve();
    }
  };
  await consumer.start({
    topic: spec.topic,
    groupId: spec.groupId,
    mode: 'each-message',
    commitEvery: definition.commitEvery,
    onMessage(value) {
      const receivedAtMs = performance.now();
      observed.consumedRecords++;
      if (value !== null) {
        observed.consumedBytes += value.length;
        const intendedAtMs = value.length >= stampBytes ? value.readDoubleLE(0) : Number.NaN;
        if (intendedAtMs >= measuredFromMs) {
          latency.record(receivedAtMs - intendedAtMs);
        }
      }
      onDelivered();
    },
    onBatch() {
      allArrived.reject(new Error('E1 needs per-record delivery to read send times'));
    },
  });

  // An object, not a `let`: TypeScript does not see the assignment in sendDue() and would type the read after the
  // await as always null.
  const measured: { window: MeasurementWindow | null } = { window: null };
  let nextIndex = 0;
  let ackedCount = 0;
  const scheduleStartMs = performance.now();
  measuredFromMs = scheduleStartMs + spec.warmupCount * intervalMs;

  const onAcked = (): void => {
    ackedCount++;
    observed.producedRecords++;
    if (ackedCount === totalCount) {
      allAcked.resolve();
    }
  };
  const onSendFailed = (error: unknown): void => {
    allArrived.reject(error);
    allAcked.reject(error);
  };

  const sendDue = (): void => {
    const nowMs = performance.now();
    while (nextIndex < totalCount) {
      const intendedAtMs = scheduleStartMs + nextIndex * intervalMs;
      if (intendedAtMs > nowMs) {
        break;
      }
      const send = outgoing[nextIndex];
      const record = send?.[0];
      if (send === undefined || record === undefined) {
        onSendFailed(new Error(`missing outgoing record ${nextIndex}`));
        return;
      }
      if (nextIndex === spec.warmupCount) {
        measured.window = startMeasurement();
      }
      record.value.writeDoubleLE(intendedAtMs, 0);
      void producer.send(spec.topic, send).then(onAcked, onSendFailed);
      nextIndex++;
    }
    if (nextIndex < totalCount) {
      setTimeout(sendDue, Math.max(0, scheduleStartMs + nextIndex * intervalMs - performance.now()));
    }
  };
  if (totalCount === 0) {
    allArrived.resolve();
    allAcked.resolve();
  } else {
    sendDue();
  }

  await allArrived.promise;
  const metrics = (measured.window ?? startMeasurement()).finish({
    records: measuredCount,
    bytes: measuredValueBytes,
    latency,
  });
  await allAcked.promise;
  return metrics;
};
