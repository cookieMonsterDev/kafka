import { describe, expect, it } from 'vitest';
import { filterAbortedMessages, type FilterableMessage } from './filter-aborted-messages.js';

function message(offset: bigint, extra: Partial<FilterableMessage> = {}): FilterableMessage {
  return {
    offset,
    key: extra.key ?? Buffer.from('key'),
    batchContext: extra.batchContext ?? { producerId: 103000n, inTransaction: true },
    ...extra,
  };
}

const abortedMessages: FilterableMessage[] = [
  message(4n, { key: Buffer.from([107, 101, 121]) }),
  message(5n, { key: Buffer.from([107, 101, 121]) }),
  message(6n, {
    key: Buffer.from([0, 0, 0, 0]),
    isControlRecord: true,
    batchContext: { producerId: 103000n, inTransaction: true },
  }),
];

const nontransactionalMessages: FilterableMessage[] = [
  message(7n, { batchContext: { producerId: 1n, inTransaction: false } }),
  message(8n, { batchContext: { producerId: 1n, inTransaction: false } }),
  message(9n, { batchContext: { producerId: 1n, inTransaction: false } }),
];

describe('consumer/filter-aborted-messages', () => {
  it('filters out all aborted messages, keeping the abort marker', () => {
    const abortedTransactions = [{ producerId: 103000n, firstOffset: 4n }];

    expect(filterAbortedMessages({ messages: abortedMessages, abortedTransactions })).toStrictEqual([
      expect.objectContaining({ key: Buffer.from([0, 0, 0, 0]) }),
    ]);

    expect(
      filterAbortedMessages({
        messages: [...abortedMessages, ...nontransactionalMessages],
        abortedTransactions,
      }),
    ).toStrictEqual([expect.objectContaining({ key: Buffer.from([0, 0, 0, 0]) }), ...nontransactionalMessages]);
  });

  it('filters out aborted messages with malformed keys', () => {
    const abortedTransactions = [{ producerId: 103000n, firstOffset: 4n }];
    const messages = [...abortedMessages];
    const malformed = messages[1];
    if (malformed) malformed.key = null;

    expect(
      filterAbortedMessages({
        messages: [...messages, ...nontransactionalMessages],
        abortedTransactions,
      }),
    ).toStrictEqual([expect.objectContaining({ key: Buffer.from([0, 0, 0, 0]) }), ...nontransactionalMessages]);
  });

  it('returns all committed messages', () => {
    const messages = [
      message(0n, { batchContext: { producerId: 1n, inTransaction: true } }),
      message(1n, { batchContext: { producerId: 1n, inTransaction: true } }),
    ];
    expect(filterAbortedMessages({ messages, abortedTransactions: [] })).toStrictEqual(messages);
  });

  it('returns all nontransactional messages', () => {
    expect(filterAbortedMessages({ messages: nontransactionalMessages, abortedTransactions: [] })).toStrictEqual(
      nontransactionalMessages,
    );
  });
});
