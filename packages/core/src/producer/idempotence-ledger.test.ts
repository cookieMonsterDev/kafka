import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { Deadline } from '../utils/deadline';
import { createIdempotenceLedger, partitionKey, type ProducerIdentity } from './idempotence-ledger';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const topic = 'topic-name';

/** InitProducerId against an in-memory broker: one producer id whose epoch rises on every call. */
function inMemoryInitProducerId() {
  return vi.fn((current: ProducerIdentity) =>
    Promise.resolve({ producerId: 1000n, producerEpoch: current.producerEpoch + 1 }),
  );
}

function createLedger(options: { canBumpEpoch?: () => boolean } = {}) {
  const initProducerId = inMemoryInitProducerId();
  const ledger = createIdempotenceLedger({ logger: silentLogger, initProducerId, ...options });
  return { ledger, initProducerId };
}

async function initializedLedger(options: { canBumpEpoch?: () => boolean } = {}) {
  const created = createLedger(options);
  await created.ledger.initProducerId();
  return created;
}

/** One send's record counts on `topic`, by partition. */
function records(recordCounts: Record<number, number>) {
  const byPartition = new Map<number, { length: number }>();
  for (const [partition, length] of Object.entries(recordCounts)) byPartition.set(Number(partition), { length });
  return new Map([[topic, byPartition]]);
}

function keys(...partitions: number[]): Set<string> {
  return new Set(partitions.map((partition) => partitionKey(topic, partition)));
}

function track(promise: Promise<void> | undefined): { settled: () => boolean } {
  let settled = false;
  void promise?.then(
    () => (settled = true),
    () => (settled = true),
  );
  return { settled: () => settled };
}

function nextTurn(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

describe('producer/idempotenceLedger', () => {
  it('initializes the producer id, then sends the current identity on later calls', async () => {
    const { ledger, initProducerId } = createLedger();
    expect(ledger.isInitialized()).toBe(false);

    await ledger.initProducerId();
    expect(initProducerId).toHaveBeenLastCalledWith({ producerId: -1n, producerEpoch: -1 });
    expect(ledger.isInitialized()).toBe(true);
    expect([ledger.getProducerId(), ledger.getProducerEpoch()]).toEqual([1000n, 0]);

    await ledger.initProducerId();
    expect(initProducerId).toHaveBeenLastCalledWith({ producerId: 1000n, producerEpoch: 0 });
    expect(ledger.getProducerEpoch()).toBe(1);
  });

  it('neither waits nor tracks anything without a producer id', () => {
    const { ledger } = createLedger();
    const lease = ledger.lease(records({ 0: 3 }));

    expect(lease.acquire()).toBeUndefined();
    expect(lease.firstSequence(topic, 0)).toBeUndefined();
    expect(lease.retryUnderNewEpoch()).toBe(false);
    expect(lease.release(keys())).toBeUndefined();
    ledger.requestEpochBump();
    expect(ledger.isEpochBumpRequested()).toBe(false);
  });

  describe('sequences', () => {
    it('hands each send the next range on a partition, independently per partition', async () => {
      const { ledger } = await initializedLedger();

      const first = ledger.lease(records({ 0: 3, 1: 2 }));
      await first.acquire();
      expect([first.firstSequence(topic, 0), first.firstSequence(topic, 1)]).toEqual([0, 0]);
      first.release(keys(0, 1));

      const second = ledger.lease(records({ 0: 1, 1: 1 }));
      await second.acquire();
      expect([second.firstSequence(topic, 0), second.firstSequence(topic, 1)]).toEqual([3, 2]);
    });

    it('keeps a range across retries until the send settles', async () => {
      const { ledger } = await initializedLedger();
      const lease = ledger.lease(records({ 0: 3 }));
      await lease.acquire();

      expect(lease.firstSequence(topic, 0)).toBe(0);
      expect(lease.firstSequence(topic, 0)).toBe(0);
      lease.release(keys(0));

      const next = ledger.lease(records({ 0: 1 }));
      await next.acquire();
      expect(next.firstSequence(topic, 0)).toBe(3);
    });

    it('rotates to 0 only after the int32 max has been used', async () => {
      const { ledger } = await initializedLedger();
      const firstSequences: (number | undefined)[] = [];
      for (const recordCount of [2 ** 31 - 1, 1, 1]) {
        const lease = ledger.lease(records({ 0: recordCount }));
        await lease.acquire();
        firstSequences.push(lease.firstSequence(topic, 0));
        lease.release(keys(0));
      }

      expect(firstSequences).toEqual([0, 2 ** 31 - 1, 0]);
    });

    it('keeps every range of a send that succeeded', async () => {
      const { ledger } = await initializedLedger();
      const succeeded = ledger.lease(records({ 0: 2 }));
      await succeeded.acquire();
      succeeded.firstSequence(topic, 0);
      succeeded.release();

      const next = ledger.lease(records({ 0: 1 }));
      await next.acquire();
      expect(next.firstSequence(topic, 0)).toBe(2);
    });

    it('hands back the ranges of partitions the broker never acked', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      const failed = ledger.lease(records({ 0: 2, 1: 3 }));
      await failed.acquire();
      failed.firstSequence(topic, 0);
      failed.firstSequence(topic, 1);
      failed.release(keys(1));

      const next = ledger.lease(records({ 0: 1, 1: 1 }));
      await next.acquire();
      expect([next.firstSequence(topic, 0), next.firstSequence(topic, 1)]).toEqual([0, 3]);
      expect(initProducerId).toHaveBeenCalledTimes(1);
    });

    it('bumps the epoch instead when an unacked range may be on the log', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      const failed = ledger.lease(records({ 0: 2, 1: 3 }));
      await failed.acquire();
      failed.firstSequence(topic, 0);
      failed.firstSequence(topic, 1);
      failed.markMaybeWritten(keys(0));
      failed.release(keys(1));

      const next = ledger.lease(records({ 0: 1, 1: 1 }));
      await next.acquire();
      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect(ledger.getProducerEpoch()).toBe(1);
      expect([next.firstSequence(topic, 0), next.firstSequence(topic, 1)]).toEqual([0, 0]);
    });

    it('keeps the range of a possibly-written request that a retry got acked', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      const lease = ledger.lease(records({ 0: 2 }));
      await lease.acquire();
      lease.firstSequence(topic, 0);
      lease.markMaybeWritten(keys(0));
      lease.release(keys(0));

      const next = ledger.lease(records({ 0: 1 }));
      await next.acquire();
      expect(next.firstSequence(topic, 0)).toBe(2);
      expect(initProducerId).toHaveBeenCalledTimes(1);
    });

    it("does not hand back an older epoch's range after the producer id was re-initialized", async () => {
      const { ledger } = await initializedLedger();
      const acked = ledger.lease(records({ 0: 5 }));
      await acked.acquire();
      acked.firstSequence(topic, 0);
      acked.release(keys(0));

      const failed = ledger.lease(records({ 0: 1 }));
      await failed.acquire();
      expect(failed.firstSequence(topic, 0)).toBe(5);
      await ledger.initProducerId();
      failed.release(keys());

      const next = ledger.lease(records({ 0: 1 }));
      await next.acquire();
      expect(next.firstSequence(topic, 0)).toBe(0);
    });

    it('restarts sequences and settles a requested bump when it adopts the identity a transaction ended with', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      const sent = ledger.lease(records({ 0: 5 }));
      await sent.acquire();
      sent.firstSequence(topic, 0);
      sent.release();
      ledger.requestEpochBump();

      ledger.adoptBumpedIdentity({ producerId: 1000n, producerEpoch: 2 });

      expect(ledger.getProducerEpoch()).toBe(2);
      expect(ledger.isEpochBumpRequested()).toBe(false);
      const next = ledger.lease(records({ 0: 1 }));
      await next.acquire();
      expect(next.firstSequence(topic, 0)).toBe(0);
      expect(initProducerId).toHaveBeenCalledOnce();
    });
  });

  describe('partition gates', () => {
    it('lets sends on different partitions overlap', async () => {
      const { ledger } = await initializedLedger();
      const first = ledger.lease(records({ 0: 1 }));
      await first.acquire();

      expect(ledger.lease(records({ 1: 1 })).acquire()).toBeUndefined();
    });

    it("reads the deadline's signal only when a send has to wait for its gate", async () => {
      const { ledger } = await initializedLedger();
      const deadline = new Deadline();
      const signal = vi.spyOn(deadline, 'signal', 'get');

      expect(ledger.lease(records({ 0: 1 })).acquire(deadline)).toBeUndefined();
      expect(signal).not.toHaveBeenCalled();

      void ledger.lease(records({ 0: 1 })).acquire(deadline);
      expect(signal).toHaveBeenCalledOnce();
    });

    it('makes a send wait for its partition until the holder settles, then continues its sequence', async () => {
      const { ledger } = await initializedLedger();
      const first = ledger.lease(records({ 0: 4 }));
      await first.acquire();
      first.firstSequence(topic, 0);

      const second = ledger.lease(records({ 0: 1 }));
      const secondAcquired = second.acquire();
      const secondState = track(secondAcquired);
      await nextTurn();
      expect(secondState.settled()).toBe(false);

      first.release(keys(0));
      await secondAcquired;
      expect(second.firstSequence(topic, 0)).toBe(4);
    });

    it('takes free gates without a promise when no bump is due, and none once it holds them', async () => {
      const { ledger } = await initializedLedger();
      const lease = ledger.lease(records({ 0: 1, 1: 1 }));

      expect(lease.acquire()).toBeUndefined();
      expect(lease.firstSequence(topic, 0)).toBe(0);
      expect(lease.acquire()).toBeUndefined();
      expect(ledger.lease(records({ 1: 1 })).acquire()).toBeInstanceOf(Promise);
    });

    it('gives back the free gates it took when one of them is held, then waits for all', async () => {
      const { ledger } = await initializedLedger();
      const holder = ledger.lease(records({ 1: 1 }));
      expect(holder.acquire()).toBeUndefined();

      const waiting = ledger.lease(records({ 0: 1, 1: 1 }));
      const waitingAcquired = waiting.acquire();
      expect(waitingAcquired).toBeInstanceOf(Promise);
      const other = ledger.lease(records({ 0: 1 }));
      expect(other.acquire()).toBeUndefined();

      other.release();
      holder.release();
      await waitingAcquired;
      expect(waiting.firstSequence(topic, 0)).toBe(0);
    });

    it('acquires shared partitions in one order, so crossing sends cannot deadlock', async () => {
      const { ledger } = await initializedLedger();
      const crossing = [
        [1, 0],
        [0, 1],
      ].map(async (partitions) => {
        const lease = ledger.lease(new Map([[topic, new Map(partitions.map((partition) => [partition, [1]]))]]));
        await lease.acquire();
        await nextTurn();
        lease.release(keys(0, 1));
      });

      await expect(Promise.all(crossing)).resolves.toEqual([undefined, undefined]);
    });

    it('keeps a queued send waiting past the lock timeout, until its deadline expires', async () => {
      vi.useFakeTimers();
      try {
        const { ledger } = await initializedLedger();
        await ledger.lease(records({ 0: 1 })).acquire();

        const deadline = new Deadline();
        const waiting = ledger.lease(records({ 0: 1 })).acquire(deadline);
        const waitingState = track(waiting);

        // A leader outage longer than the 0xffff ms InitProducerId lock timeout must not fail queued sends.
        await vi.advanceTimersByTimeAsync(5 * 60_000);
        expect(waitingState.settled()).toBe(false);

        deadline.expire(new Error('delivery deadline'));
        await expect(waiting).rejects.toThrow('delivery deadline');
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe('epoch bumps', () => {
    it('bumps once on the next acquire after a request, then not again', async () => {
      const { ledger, initProducerId } = await initializedLedger();

      ledger.requestEpochBump();
      await Promise.all([ledger.bumpEpochIfRequested(), ledger.lease(records({ 0: 1 })).acquire()]);
      await ledger.bumpEpochIfRequested();

      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect(ledger.getProducerEpoch()).toBe(1);
      expect(ledger.isEpochBumpRequested()).toBe(false);
    });

    it('keeps the request when the bump fails so the next send tries again', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      initProducerId.mockRejectedValueOnce(new Error('coordinator down'));

      ledger.requestEpochBump();
      await expect(ledger.lease(records({ 0: 1 })).acquire()).rejects.toThrow('coordinator down');
      await ledger.lease(records({ 0: 1 })).acquire();

      expect(ledger.getProducerEpoch()).toBe(1);
    });

    it('holds new sends back until the sends holding gates settle, then bumps once', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      const holder = ledger.lease(records({ 0: 1 }));
      await holder.acquire();

      ledger.requestEpochBump();
      // Partition 1 is free, but its send must not start while partition 0's old-epoch send is in flight.
      const waiting = [ledger.lease(records({ 1: 1 })).acquire()!, ledger.lease(records({ 2: 1 })).acquire()!];
      const waitingState = track(waiting[0]);
      await nextTurn();
      expect(waitingState.settled()).toBe(false);
      expect(initProducerId).toHaveBeenCalledTimes(1);

      holder.release(keys(0));
      await Promise.all(waiting);
      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect(ledger.getProducerEpoch()).toBe(1);
    });

    it('does not let a send queued on a gate slip in ahead of the bump its holder requested', async () => {
      const { ledger } = await initializedLedger();
      const holder = ledger.lease(records({ 0: 1 }));
      await holder.acquire();

      let epochWhenAcquired: number | undefined;
      const queued = ledger
        .lease(records({ 0: 1 }))
        .acquire()
        ?.then(() => (epochWhenAcquired = ledger.getProducerEpoch()));

      ledger.requestEpochBump();
      holder.release(keys(0));
      await queued;
      expect(epochWhenAcquired).toBe(1);
    });

    it('gives up waiting for the drain when the send is aborted', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      await ledger.lease(records({ 0: 1 })).acquire();
      ledger.requestEpochBump();

      const deadline = new Deadline();
      const waiting = ledger.lease(records({ 1: 1 })).acquire(deadline);
      deadline.expire(new Error('delivery deadline'));

      await expect(waiting).rejects.toThrow('delivery deadline');
      expect(initProducerId).toHaveBeenCalledTimes(1);
    });

    it('defers a requested bump while it may not run', async () => {
      let canBumpEpoch = false;
      const { ledger, initProducerId } = await initializedLedger({ canBumpEpoch: () => canBumpEpoch });

      ledger.requestEpochBump();
      const lease = ledger.lease(records({ 0: 1 }));
      await lease.acquire();
      expect(initProducerId).toHaveBeenCalledTimes(1);
      lease.release(keys(0));

      canBumpEpoch = true;
      await ledger.bumpEpochIfRequested();
      expect(initProducerId).toHaveBeenCalledTimes(2);
    });
  });

  describe('when the broker lost the producer id', () => {
    it('gives the gates back so one bump runs once the other sends drain', async () => {
      const { ledger, initProducerId } = await initializedLedger();
      const lost = ledger.lease(records({ 0: 1 }));
      const retrying = ledger.lease(records({ 1: 1 }));
      await Promise.all([lost.acquire(), retrying.acquire()]);
      lost.firstSequence(topic, 0);
      retrying.firstSequence(topic, 1);

      expect(lost.retryUnderNewEpoch()).toBe(true);
      // Its gate is free again, but no send may take a sequence until the other send drains.
      const lostAgain = lost.acquire();
      const lostState = track(lostAgain);
      await nextTurn();
      expect(lostState.settled()).toBe(false);
      expect(retrying.firstSequence(topic, 1)).toBe(0);
      expect(initProducerId).toHaveBeenCalledTimes(1);

      retrying.release(keys(1));
      await lostAgain;
      expect(initProducerId).toHaveBeenCalledTimes(2);
      expect([ledger.getProducerEpoch(), lost.firstSequence(topic, 0)]).toEqual([1, 0]);
    });

    it('keeps the gates and fails the send when the epoch cannot be bumped now', async () => {
      const { ledger } = await initializedLedger({ canBumpEpoch: () => false });
      const lost = ledger.lease(records({ 0: 1 }));
      await lost.acquire();

      expect(lost.retryUnderNewEpoch()).toBe(false);
      expect(ledger.isEpochBumpRequested()).toBe(true);
      expect(lost.acquire()).toBeUndefined();
    });
  });
});
