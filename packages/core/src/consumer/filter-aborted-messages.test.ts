import { describe, expect, it } from 'vitest';
import { filterAbortedMessages, type FilterableMessage } from './filter-aborted-messages';

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

    expect(
      filterAbortedMessages({
        messages: [...abortedMessages, ...nontransactionalMessages],
        abortedTransactions,
        excludeControlRecords: true,
      }),
    ).toStrictEqual(nontransactionalMessages);
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
    expect(filterAbortedMessages({ messages, abortedTransactions: [] })).toBe(messages);
  });

  it('returns all nontransactional messages', () => {
    expect(filterAbortedMessages({ messages: nontransactionalMessages, abortedTransactions: [] })).toStrictEqual(
      nontransactionalMessages,
    );
  });

  it('returns the same array instance when abortedTransactions is omitted or empty', () => {
    expect(filterAbortedMessages({ messages: abortedMessages })).toBe(abortedMessages);
    expect(filterAbortedMessages({ messages: abortedMessages, abortedTransactions: null })).toBe(abortedMessages);
    expect(filterAbortedMessages({ messages: abortedMessages, abortedTransactions: [] })).toBe(abortedMessages);
  });

  it('treats a four-null-byte string key as an abort marker', () => {
    const abortedTransactions = [{ producerId: 103000n, firstOffset: 4n }];
    const messages = [
      message(4n),
      message(5n, {
        key: '\0\0\0\0',
        isControlRecord: true,
        batchContext: { producerId: 103000n, inTransaction: true },
      }),
      message(6n, { batchContext: { producerId: 1n, inTransaction: false } }),
    ];

    expect(filterAbortedMessages({ messages, abortedTransactions })).toStrictEqual([
      expect.objectContaining({ offset: 5n }),
      expect.objectContaining({ offset: 6n }),
    ]);
  });

  it('handles consecutive aborted transactions from different producers', () => {
    const messages = [
      message(1n, { batchContext: { producerId: 1n, inTransaction: true } }),
      message(2n, {
        key: Buffer.from([0, 0, 0, 0]),
        isControlRecord: true,
        batchContext: { producerId: 1n, inTransaction: true },
      }),
      message(3n, { batchContext: { producerId: 2n, inTransaction: true } }),
      message(4n, {
        key: Buffer.from([0, 0, 0, 0]),
        isControlRecord: true,
        batchContext: { producerId: 2n, inTransaction: true },
      }),
      message(5n, { batchContext: { producerId: 3n, inTransaction: true } }),
    ];

    expect(
      filterAbortedMessages({
        messages,
        abortedTransactions: [
          { producerId: 1n, firstOffset: 1n },
          { producerId: 2n, firstOffset: 3n },
        ],
      }),
    ).toStrictEqual([
      expect.objectContaining({ offset: 2n }),
      expect.objectContaining({ offset: 4n }),
      expect.objectContaining({ offset: 5n }),
    ]);
  });
});
