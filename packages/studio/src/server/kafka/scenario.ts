import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import type { DebugScenario, ScenarioStepResult } from '../../shared/contracts/debug';
import { matchesPredicates } from '../../shared/message-query';
import type { PooledAdmin } from './admin-pool';
import { openAssignedStream, toMessageRecord, type MessageConsumerFactory } from './messages';
import type { PooledProducer } from './produce';
import { searchableRecord } from './scan';

/** The run ID substitutes {{id}} throughout one run; expectations start at offsets captured before any produce. */
export async function runScenario(
  scenario: DebugScenario,
  admin: PooledAdmin,
  producer: PooledProducer,
  factory: MessageConsumerFactory,
  profile: string | null,
  signal: AbortSignal,
  report: (result: ScenarioStepResult) => void,
): Promise<{ runId: string; results: ScenarioStepResult[] }> {
  const runId = randomUUID();
  const substitute = (text: string): string => text.replaceAll('{{id}}', runId);
  const topics = [...new Set(scenario.steps.filter((step) => step.kind === 'expect').map((step) => step.topic))];
  const starts = new Map<string, { partition: number; offset: bigint }[]>();
  for (const topic of topics)
    starts.set(
      topic,
      (await admin.fetchTopicOffsets(topic)).map((range) => ({ partition: range.partition, offset: range.high })),
    );
  const results: ScenarioStepResult[] = [];
  for (const [index, step] of scenario.steps.entries()) {
    signal.throwIfAborted();
    try {
      if (step.kind === 'produce') {
        await producer.send({
          topic: step.topic,
          messages: [
            {
              key: step.key === undefined ? undefined : substitute(step.key),
              value: step.value === null ? null : substitute(step.value),
              headers:
                step.headers === undefined
                  ? undefined
                  : Object.fromEntries(Object.entries(step.headers).map(([key, value]) => [key, substitute(value)])),
            },
          ],
          signal,
        });
      } else {
        const timeout = AbortSignal.timeout(step.timeoutMs);
        const combined = AbortSignal.any([signal, timeout]);
        const consumer = factory(profile).consumer();
        let matched = false;
        const predicates = step.predicates.map((entry) => ({
          ...entry,
          value: typeof entry.value === 'string' ? substitute(entry.value) : entry.value,
        }));
        try {
          await consumer.connect();
          const batches = await openAssignedStream(consumer, step.topic, starts.get(step.topic) ?? [], {
            signal: combined,
          });
          for await (const batch of batches) {
            combined.throwIfAborted();
            const start = starts.get(step.topic)?.find((entry) => entry.partition === batch.partition)?.offset;
            if (
              batch.messages.some(
                (message) =>
                  start !== undefined &&
                  message.offset >= start &&
                  matchesPredicates(
                    searchableRecord({ topic: step.topic, ...toMessageRecord(batch.partition, message) }),
                    predicates,
                  ),
              )
            ) {
              matched = true;
              break;
            }
            combined.throwIfAborted();
          }
        } finally {
          await consumer.disconnect();
        }
        if (!matched) throw new Error('No matching event before the expectation deadline');
      }
      const result: ScenarioStepResult = { index, status: 'passed', message: `${step.kind} ${step.topic}` };
      results.push(result);
      report(result);
    } catch (error) {
      const result: ScenarioStepResult = {
        index,
        status: 'failed',
        message: error instanceof Error ? error.message : 'Step failed',
      };
      results.push(result);
      report(result);
      break;
    }
    await delay(0, undefined, { signal });
  }
  return { runId, results };
}
