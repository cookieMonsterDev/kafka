import type { KafkaMessage } from '@cookiemonsterdev/kafka-core';
import type { Batch } from '@cookiemonsterdev/kafka-core';
import type { PooledMessageConsumer } from './messages';

export type FakeMessageConsumerOverrides = Partial<PooledMessageConsumer>;

/**
 * A {@link PooledMessageConsumer} where every method throws unless explicitly stubbed — mirrors
 * `create-fake-admin.ts` for the same reason: a test only stubs the calls its scenario actually
 * needs, so an unexpected call fails immediately and by name.
 */
export function createFakeMessageConsumer(overrides: FakeMessageConsumerOverrides = {}): PooledMessageConsumer {
  const handler: ProxyHandler<PooledMessageConsumer> = {
    get(_target, prop) {
      if (prop === 'then') return undefined;
      if (Object.hasOwn(overrides, prop)) {
        return (overrides as Record<PropertyKey, unknown>)[prop as string];
      }
      return (...args: unknown[]) => {
        throw new Error(
          `createFakeMessageConsumer: unstubbed call to Consumer.${String(prop)}(${args.map((arg) => JSON.stringify(arg)).join(', ')})`,
        );
      };
    },
  };
  return new Proxy({} as PooledMessageConsumer, handler);
}

export interface FakeBatchInput {
  readonly topic: string;
  readonly partition: number;
  readonly highWatermark: bigint;
  readonly fetchedOffset: bigint;
  readonly messages: readonly KafkaMessage[];
}

/** Builds a {@link Batch}-shaped fixture without going through the real (decode-from-bytes) constructor — a plain, public-fields-only object satisfies the type structurally. */
export function createFakeBatch(input: FakeBatchInput): Batch {
  const { topic, partition, highWatermark, fetchedOffset, messages } = input;
  return {
    topic,
    partition,
    highWatermark,
    fetchedOffset,
    rawMessages: messages,
    messagesWithinOffset: [...messages],
    messages: [...messages],
    isEmpty: () => messages.length === 0,
    isEmptyIncludingFiltered: () => messages.length === 0,
    isEmptyDueToFiltering: () => false,
    isEmptyControlRecord: () => false,
    isEmptyDueToLogCompactedMessages: () => false,
    firstOffset: () => messages.at(0)?.offset ?? null,
    lastOffset: () => messages.at(-1)?.offset ?? highWatermark - 1n,
    offsetLag: () => highWatermark - 1n - (messages.at(-1)?.offset ?? highWatermark - 1n),
    offsetLagLow: () => highWatermark - 1n - (messages.at(0)?.offset ?? highWatermark - 1n),
  };
}

export function createFakeKafkaMessage(overrides: Partial<KafkaMessage> = {}): KafkaMessage {
  return {
    magicByte: 2,
    attributes: 0,
    timestamp: 0n,
    offset: 0n,
    key: null,
    value: null,
    headers: {},
    isControlRecord: false,
    batchContext: {} as KafkaMessage['batchContext'],
    byteSize: 0,
    ...overrides,
  };
}
