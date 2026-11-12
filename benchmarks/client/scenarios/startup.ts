import { performance } from 'node:perf_hooks';
import type { ConsumeOptions } from '../adapters/types.ts';
import { startMeasurement } from '../measure.ts';
import type { RunMetrics } from '../spec.ts';
import { createSignal, pooledRecord, type ScenarioContext, type ScenarioRunner } from './harness.ts';

/**
 * L1: milliseconds from process start to the first acked produce of one record, and to a new
 * consumer receiving that record. Node and Bun both start `performance.now()` at process start
 * (checked on Node 26.13 and Bun 1.4.4), so its value is the elapsed time, module loading included.
 * The window and CPU figures also count from process start and describe that single record.
 */
export const runStartupScenario: ScenarioRunner = async (context: ScenarioContext): Promise<RunMetrics> => {
  const { spec, definition, observed } = context;
  const window = startMeasurement({ fromProcessStart: true });
  const record = pooledRecord(context.records, 0);

  const producer = await context.clients.connectProducer(context.adapter, context.adapterOptions);
  await producer.send(spec.topic, [record]);
  const firstAckedProduceMs = performance.now();
  observed.producedRecords = 1;

  const firstConsumed = createSignal<number>();
  const onDelivered = (count: number, bytes: number): void => {
    const deliveredAtMs = performance.now();
    observed.consumedRecords += count;
    observed.consumedBytes += bytes;
    if (count > 0) {
      firstConsumed.resolve(deliveredAtMs);
    }
  };
  const consumeOptions: ConsumeOptions = {
    topic: spec.topic,
    groupId: spec.groupId,
    mode: 'each-message',
    commitEvery: definition.commitEvery,
    onMessage: (value) => onDelivered(1, value === null ? 0 : value.length),
    onBatch: onDelivered,
  };
  const consumer = await context.clients.connectConsumer(context.adapter, context.adapterOptions);
  await consumer.start(consumeOptions);
  const firstConsumedRecordMs = await firstConsumed.promise;

  return window.finish({
    records: 1,
    bytes: record.value.length,
    latency: null,
    startup: { firstAckedProduceMs, firstConsumedRecordMs },
  });
};
