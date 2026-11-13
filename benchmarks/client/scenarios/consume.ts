import { type MeasurementWindow, startMeasurement } from '../measure.ts';
import type { RunMetrics } from '../spec.ts';
import { createSignal, type ScenarioContext } from './harness.ts';

/**
 * Consume scenarios (C1 to C4) on a topic pre-seeded with `warmupCount + messageCount` records.
 * The first `warmupCount` records are warm-up; the window opens when the delivered count reaches
 * them (with `warmupCount` 0, before `start()`, so it includes joining the group) and closes
 * when every record arrived.
 * A batch that crosses the warm-up boundary counts as warm-up, so with `each-batch` the window can
 * hold slightly fewer than `messageCount` records; `metrics.records` reports the exact number.
 */
export async function runConsumeScenario(context: ScenarioContext): Promise<RunMetrics> {
  const { spec, definition, observed } = context;
  const totalRecords = spec.warmupCount + spec.messageCount;
  const done = createSignal<RunMetrics>();
  let window: MeasurementWindow | null = null;
  let recordsAtWindowStart = 0;
  let bytesAtWindowStart = 0;
  let finished = false;

  const openWindow = (): void => {
    recordsAtWindowStart = observed.consumedRecords;
    bytesAtWindowStart = observed.consumedBytes;
    window = startMeasurement();
  };

  const afterDelivery = (): void => {
    if (window === null && observed.consumedRecords >= spec.warmupCount) {
      openWindow();
    }
    if (finished || window === null || observed.consumedRecords < totalRecords) {
      return;
    }
    finished = true;
    const metrics = window.finish({
      records: observed.consumedRecords - recordsAtWindowStart,
      bytes: observed.consumedBytes - bytesAtWindowStart,
      latency: null,
    });
    done.resolve(metrics);
  };

  const consumer = await context.clients.connectConsumer(context.adapter, context.adapterOptions);
  if (spec.warmupCount === 0) {
    openWindow();
  }
  await consumer.start({
    topic: spec.topic,
    groupId: spec.groupId,
    mode: definition.consumeMode,
    commitEvery: definition.commitEvery,
    onMessage(value) {
      observed.consumedRecords++;
      observed.consumedBytes += value === null ? 0 : value.length;
      afterDelivery();
    },
    onBatch(count, bytes) {
      observed.consumedRecords += count;
      observed.consumedBytes += bytes;
      afterDelivery();
    },
  });
  return done.promise;
}
