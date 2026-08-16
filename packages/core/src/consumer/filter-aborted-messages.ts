const ABORTED_MESSAGE_KEY = Buffer.from([0, 0, 0, 0]);

function isAbortMarker(message: { key?: Buffer | string | null }): boolean {
  if (!message.key) return false;
  return Buffer.from(message.key).equals(ABORTED_MESSAGE_KEY);
}

export interface FilterableMessage {
  offset: bigint;
  key?: Buffer | string | null;
  isControlRecord?: boolean;
  batchContext: {
    producerId: bigint;
    inTransaction: boolean;
  };
}

export interface AbortedTransaction {
  producerId: bigint;
  firstOffset: bigint;
}

/**
 * Drop records that belong to an aborted transaction. The aborted range starts at
 * `abortedTransactions[].firstOffset` and ends at the abort-marker control record
 * (`key = 0x00000000`) for that producer. See https://kafka.apache.org/documentation/#controlbatch
 */
export function filterAbortedMessages<T extends FilterableMessage>({
  messages,
  abortedTransactions,
}: {
  messages: readonly T[];
  abortedTransactions?: readonly AbortedTransaction[] | null;
}): T[] {
  if (!abortedTransactions || abortedTransactions.length === 0) {
    return [...messages];
  }

  const currentAbortedTransactions = new Map<bigint, true>();
  const remainingAbortedTransactions = [...abortedTransactions];

  return messages.filter((message) => {
    const nextAborted = remainingAbortedTransactions[0];
    if (nextAborted && message.offset >= nextAborted.firstOffset) {
      remainingAbortedTransactions.shift();
      currentAbortedTransactions.set(nextAborted.producerId, true);
    }

    const { producerId, inTransaction } = message.batchContext;

    if (isAbortMarker(message)) {
      currentAbortedTransactions.delete(producerId);
    } else if (currentAbortedTransactions.has(producerId) && inTransaction) {
      return false;
    }

    return true;
  });
}
