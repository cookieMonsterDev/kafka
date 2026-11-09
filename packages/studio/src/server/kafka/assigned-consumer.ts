import type { Batch } from '@cookiemonsterdev/kafka-core';
import type { PooledMessageConsumer } from './messages';

export interface AssignedCursor {
  readonly partition: number;
  readonly offset: bigint;
}

/** Owns an assigned consumer until iteration ends, fails, or is cancelled. */
export async function* readAssignedMessages(
  consumer: PooledMessageConsumer,
  topic: string,
  cursors: readonly AssignedCursor[],
  config: { readonly signal?: AbortSignal } = {},
): AsyncIterableIterator<Batch> {
  const controller = new AbortController();
  const signal = config.signal === undefined ? controller.signal : AbortSignal.any([config.signal, controller.signal]);
  let iterator: AsyncIterableIterator<Batch> | undefined;
  try {
    signal.throwIfAborted();
    await consumer.connect({ signal });
    signal.throwIfAborted();
    await consumer.assign(cursors.map((cursor) => ({ topic, partition: cursor.partition })));
    signal.throwIfAborted();
    iterator = consumer.stream({ signal });
    // Requesting the first item initializes assignment synchronously; seek must follow it.
    const first = iterator.next();
    // A seek failure can enter cleanup before the first request rejects.
    void first.catch(() => {});
    for (const cursor of cursors) consumer.seek({ topic, partition: cursor.partition, offset: cursor.offset });
    let item = await first;
    while (!item.done) {
      signal.throwIfAborted();
      yield item.value;
      item = await iterator.next();
    }
  } finally {
    // Wake a pending next() before return(), including when setup failed after priming.
    controller.abort();
    try {
      await iterator?.return?.();
    } finally {
      await consumer.disconnect();
    }
  }
}
