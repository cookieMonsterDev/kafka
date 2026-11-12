import { performance } from 'node:perf_hooks';
import type { BenchProducer, BenchRecord } from '../adapters/types.ts';
import { LatencyRecorder, startMeasurement } from '../measure.ts';
import { sumValueBytes } from '../adapters/shared.ts';
import type { RunMetrics } from '../spec.ts';
import { type ObservedCounts, pooledRecord, type ScenarioContext } from './harness.ts';

interface SendBatches {
  /** Full `recordsPerSend` batches cycling through the pool, built before any send. */
  batches: readonly (readonly BenchRecord[])[];
  batchValueBytes: Float64Array;
}

function buildSendBatches(records: readonly BenchRecord[], recordsPerSend: number): SendBatches {
  const batches: BenchRecord[][] = [];
  const batchValueBytes = new Float64Array(records.length);
  for (let batchIndex = 0; batchIndex < records.length; batchIndex++) {
    const batch: BenchRecord[] = [];
    for (let offset = 0; offset < recordsPerSend; offset++) {
      const record = pooledRecord(records, batchIndex * recordsPerSend + offset);
      batch.push(record);
      batchValueBytes[batchIndex] = (batchValueBytes[batchIndex] ?? 0) + record.value.length;
    }
    batches.push(batch);
  }
  return { batches, batchValueBytes };
}

interface SendPhase {
  producer: BenchProducer;
  topic: string;
  sendBatches: SendBatches;
  /** Records to send in this phase; the last send carries the remainder when not a multiple. */
  recordCount: number;
  recordsPerSend: number;
  concurrency: number;
  /** Index of the first pooled batch to use, so phases continue the cycle. */
  firstBatchIndex: number;
  observed: ObservedCounts;
  latency: LatencyRecorder | null;
}

interface SendPhaseResult {
  ackedRecords: number;
  valueBytes: number;
  nextBatchIndex: number;
}

/**
 * Sends `recordCount` records with at most `concurrency` `send()` calls in flight. Each worker
 * awaits its own send before taking the next index, so in-flight work stays bounded.
 */
async function runSendPhase(phase: SendPhase): Promise<SendPhaseResult> {
  const { batches, batchValueBytes } = phase.sendBatches;
  const fullSends = Math.floor(phase.recordCount / phase.recordsPerSend);
  const remainderRecords = phase.recordCount % phase.recordsPerSend;
  const tailBatchIndex = (phase.firstBatchIndex + fullSends) % batches.length;
  const tailBatch = remainderRecords === 0 ? null : (batches[tailBatchIndex] ?? []).slice(0, remainderRecords);
  const tailValueBytes = tailBatch === null ? 0 : sumValueBytes(tailBatch);
  const totalSends = fullSends + (tailBatch === null ? 0 : 1);

  let nextSend = 0;
  let ackedRecords = 0;
  let valueBytes = 0;
  let failure: unknown = null;

  const worker = async (): Promise<void> => {
    while (nextSend < totalSends && failure === null) {
      const sendIndex = nextSend++;
      const batchIndex = (phase.firstBatchIndex + sendIndex) % batches.length;
      const isTail = sendIndex === fullSends;
      const batch = isTail && tailBatch !== null ? tailBatch : batches[batchIndex];
      if (batch === undefined) {
        throw new Error(`missing send batch ${batchIndex}`);
      }
      const startedAtMs = performance.now();
      try {
        await phase.producer.send(phase.topic, batch);
      } catch (error) {
        failure ??= error;
        throw error;
      }
      phase.latency?.record(performance.now() - startedAtMs);
      ackedRecords += batch.length;
      phase.observed.producedRecords += batch.length;
      valueBytes += isTail && tailBatch !== null ? tailValueBytes : (batchValueBytes[batchIndex] ?? 0);
    }
  };

  const workerCount = Math.min(phase.concurrency, Math.max(1, totalSends));
  const workers: Promise<void>[] = [];
  for (let index = 0; index < workerCount; index++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  return { ackedRecords, valueBytes, nextBatchIndex: (phase.firstBatchIndex + fullSends) % batches.length };
}

/**
 * Produce scenarios (P1 to P5): a warm-up of `warmupCount` records with the same call shape, then
 * a measured window of `messageCount` records. One latency sample per `send()` call.
 */
export async function runProduceScenario(context: ScenarioContext): Promise<RunMetrics> {
  const { spec, definition } = context;
  const sendBatches = buildSendBatches(context.records, definition.recordsPerSend);
  const producer = await context.clients.connectProducer(context.adapter, context.adapterOptions);
  const phaseBase = {
    producer,
    topic: spec.topic,
    sendBatches,
    recordsPerSend: definition.recordsPerSend,
    concurrency: definition.concurrency,
    observed: context.observed,
  };

  const warmup = await runSendPhase({
    ...phaseBase,
    recordCount: spec.warmupCount,
    firstBatchIndex: 0,
    latency: null,
  });

  const latency = new LatencyRecorder(Math.ceil(spec.messageCount / definition.recordsPerSend));
  const window = startMeasurement();
  const measured = await runSendPhase({
    ...phaseBase,
    recordCount: spec.messageCount,
    firstBatchIndex: warmup.nextBatchIndex,
    latency,
  });
  return window.finish({ records: measured.ackedRecords, bytes: measured.valueBytes, latency });
}
