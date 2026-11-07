import { describe, expect, it, vi } from 'vitest';
import { scenarioSchema } from '../../shared/contracts/debug';
import { createFakeAdmin } from './create-fake-admin';
import { createFakeProducer } from './create-fake-producer';
import { createFakeBatch, createFakeKafkaMessage, createFakeMessageConsumer } from './create-fake-message-consumer';
import { runScenario } from './scenario';

describe('scenario execution', () => {
  it('captures output offsets before producing and substitutes a shared run ID', async () => {
    const calls: string[] = [];
    const seek = vi.fn();
    let value: string | Buffer | null | undefined;
    const admin = createFakeAdmin({
      fetchTopicOffsets: async () => {
        calls.push('bounds');
        return [{ partition: 0, offset: 10n, low: 0n, high: 10n }];
      },
    });
    const producer = createFakeProducer({
      send: async (input) => {
        calls.push('produce');
        value = input.messages[0]?.value;
        return [];
      },
    });
    const disconnect = vi.fn(async () => {});
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect,
      assign: async () => {},
      seek,
      stream: async function* () {
        yield createFakeBatch({
          topic: 'output',
          partition: 0,
          highWatermark: 11n,
          fetchedOffset: 10n,
          messages: [createFakeKafkaMessage({ offset: 10n, value: Buffer.from(value ?? '') })],
        });
      },
    });
    const scenario = scenarioSchema.parse({
      version: 1,
      name: 'flow',
      steps: [
        { kind: 'produce', topic: 'input', value: '{"id":"{{id}}"}' },
        { kind: 'expect', topic: 'output', predicates: [{ field: 'value.id', operator: 'eq', value: '{{id}}' }] },
      ],
    });
    const result = await runScenario(
      scenario,
      admin,
      producer,
      () => ({ consumer: () => consumer }),
      null,
      new AbortController().signal,
      () => {},
    );
    expect(calls).toEqual(['bounds', 'produce']);
    expect(value).toBe(`{"id":"${result.runId}"}`);
    expect(result.results.map((step) => step.status)).toEqual(['passed', 'passed']);
    expect(seek).toHaveBeenCalledWith({ topic: 'output', partition: 0, offset: 10n });
    expect(disconnect).toHaveBeenCalledOnce();
  });
  it('stops after a failed send and reports the failed step', async () => {
    const send = vi.fn(async () => {
      throw new Error('broker unavailable');
    });
    const scenario = scenarioSchema.parse({
      version: 1,
      name: 'failure',
      steps: [
        { kind: 'produce', topic: 'input', value: 'a' },
        { kind: 'produce', topic: 'input', value: 'b' },
      ],
    });
    const result = await runScenario(
      scenario,
      createFakeAdmin(),
      createFakeProducer({ send }),
      () => {
        throw new Error('Unexpected consumer');
      },
      null,
      new AbortController().signal,
      () => {},
    );
    expect(result.results).toEqual([{ index: 0, status: 'failed', message: 'broker unavailable' }]);
    expect(send).toHaveBeenCalledOnce();
  });
});
