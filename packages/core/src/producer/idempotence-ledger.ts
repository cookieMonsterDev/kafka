import { INT_32_MAX_VALUE } from '../constants';
import type { Logger } from '../loggers/index';
import { rejectOnAbort } from '../utils/abort';
import type { Deadline } from '../utils/deadline';
import { Lock, type AcquireOptions } from '../utils/lock';

const NO_PRODUCER_ID = -1n;
const SEQUENCE_START = 0;

/** A producer id and epoch, as InitProducerId takes and returns them. */
export interface ProducerIdentity {
  producerId: bigint;
  producerEpoch: number;
}

/** What InitProducerId is sent with before the producer has an id. */
const NO_PRODUCER_IDENTITY: ProducerIdentity = { producerId: NO_PRODUCER_ID, producerEpoch: -1 };

/** One send's records by topic, then partition. Only their counts matter to the ledger. */
export type LeasedRecords = ReadonlyMap<string, ReadonlyMap<number, { readonly length: number }>>;

export function partitionKey(topic: string, partition: number): string {
  return `${topic}\0${partition}`;
}

/**
 * One send's hold on its partitions' gates and sequence numbers, from its first attempt until it
 * settles. Every method is a no-op until the producer has an id.
 */
export interface SequenceLease {
  /**
   * Takes this send's partition gates, after any pending epoch bump. Returns a promise only when it
   * has to wait for a held gate or a bump: never once the gates are held or without a producer id.
   * The wait gives up when `deadline` expires; only a wait reads its signal.
   */
  acquire: (deadline?: Deadline) => Promise<void> | undefined;
  /**
   * The first sequence of this send's records on the partition, the same on every retry under one
   * producer epoch, or `undefined` while the lease holds no gates.
   */
  firstSequence: (topic: string, partition: number) => number | undefined;
  /** Partitions (by `partitionKey`) of a failed request that the broker may still have appended. */
  markMaybeWritten: (partitionKeys: Iterable<string>) => void;
  /**
   * UNKNOWN_PRODUCER_ID: the broker dropped the producer id. Requests an epoch bump and, when it can
   * run before the next attempt, drops this send's sequences and gives its gates back so the bump
   * runs once every other send drains; the next `acquire` waits for it. Returns whether to retry.
   */
  retryUnderNewEpoch: () => boolean;
  /**
   * Ends the lease. After a send succeeded (no argument), every range stays spent. After it failed,
   * hands back the ranges of partitions not in `ackedPartitionKeys`, unless one of them may be on
   * the log: that range can be neither reused (the next send's records would be dropped as
   * duplicates) nor skipped (OUT_OF_ORDER_SEQUENCE_NUMBER), so it requests an epoch bump instead.
   */
  release: (ackedPartitionKeys?: ReadonlySet<string>) => void;
}

/**
 * The idempotent producer's identity and the rules that keep its sequence numbers valid: one send
 * at a time per partition, ranges kept across retries, and epoch bumps only once sends drain.
 */
export interface IdempotenceLedger {
  getProducerId: () => bigint;
  getProducerEpoch: () => number;
  isInitialized: () => boolean;
  /** Sends InitProducerId now, without waiting for sends to drain: for connect and between transactions. */
  initProducerId: () => Promise<void>;
  /**
   * KIP-360: a failed send may have been written, so its sequences can neither be reused nor
   * skipped. Start a new epoch (fresh sequences) before producing again. No-op until initialized.
   */
  requestEpochBump: () => void;
  isEpochBumpRequested: () => boolean;
  /** Runs a requested epoch bump once no send holds partition gates and `canBumpEpoch` allows it. */
  bumpEpochIfRequested: (options?: AcquireOptions) => Promise<void>;
  lease: (records: LeasedRecords) => SequenceLease;
}

export interface IdempotenceLedgerOptions {
  logger: Logger;
  /** Sends InitProducerId with the current identity and resolves with the new one. */
  initProducerId: (current: ProducerIdentity) => Promise<ProducerIdentity>;
  /** Whether a requested epoch bump may run now; a transaction bumps only once it ends. Defaults to always. */
  canBumpEpoch?: () => boolean;
}

/** Takes every gate at once if all are free; otherwise takes none. */
function tryAcquireGates(gates: readonly Lock[]): boolean {
  for (let index = 0; index < gates.length; index++) {
    if (!gates[index]!.tryAcquire()) {
      releaseGates(gates, index);
      return false;
    }
  }
  return true;
}

async function acquireGates(gates: readonly Lock[], signal: AbortSignal | undefined): Promise<void> {
  for (let index = 0; index < gates.length; index++) {
    try {
      await gates[index]!.acquire({ signal });
    } catch (error) {
      releaseGates(gates, index);
      throw error;
    }
  }
}

/** Releases the first `count` gates in reverse order. `Lock.release` settles synchronously. */
function releaseGates(gates: readonly Lock[], count = gates.length): void {
  for (let index = count - 1; index >= 0; index--) {
    void gates[index]!.release();
  }
}

export function createIdempotenceLedger({
  logger,
  initProducerId: sendInitProducerId,
  canBumpEpoch = () => true,
}: IdempotenceLedgerOptions): IdempotenceLedger {
  let producerId = NO_PRODUCER_ID;
  let producerEpoch = 0;
  /** Rises with every new producer id or epoch; a lease's sequences belong to the version they were taken under. */
  let identityVersion = 0;
  /** The next sequence to hand out per partition (by `partitionKey`) under the current identity. */
  const nextSequences = new Map<string, number>();

  /** Serializes InitProducerId, whether sent directly or as an epoch bump. */
  const initProducerIdLock = new Lock({ timeout: 0xffff, description: 'InitProducerId' });

  /**
   * Per-partition Produce gate: one send at a time per partition keeps sequences monotonic, while
   * distinct partitions on one broker may overlap. The broker's idempotent window is 5, but without
   * a single sender thread pipelining one partition risks OUT_OF_ORDER_SEQUENCE_NUMBER. A holder
   * keeps its gate for its whole retry cycle, which may outlast any fixed lock timeout during a
   * leader outage, so waiters have none: each is bounded by its own send's delivery deadline.
   */
  const partitionGates = new Map<string, Lock>();

  let epochBumpRequested = false;
  let epochBump: Promise<void> | undefined;
  /** Leases currently holding partition gates, i.e. with requests (and sequences) in flight. */
  let gateHolders = 0;
  const drainWaiters = new Set<() => void>();

  function isInitialized(): boolean {
    return producerId !== NO_PRODUCER_ID;
  }

  async function initProducerId(): Promise<void> {
    await initProducerIdLock.acquire();
    try {
      const current = isInitialized() ? { producerId, producerEpoch } : NO_PRODUCER_IDENTITY;
      const next = await sendInitProducerId(current);
      producerId = next.producerId;
      producerEpoch = next.producerEpoch;
      identityVersion += 1;
      nextSequences.clear();
      epochBumpRequested = false;
      // Gates stay: a send still holding one must keep its partition to itself across the bump.
      logger.debug('Initialized producer id & epoch', { producerId: producerId.toString(), producerEpoch });
    } finally {
      await initProducerIdLock.release();
    }
  }

  function requestEpochBump(): void {
    if (isInitialized()) epochBumpRequested = true;
  }

  /** A requested bump that may run now, once the gate holders drain. */
  function isEpochBumpDue(): boolean {
    return epochBumpRequested && canBumpEpoch();
  }

  /**
   * Like the Java client, bump only once in-flight sends drain: bumping under a send that is still
   * retrying would resend its possibly-written batch with new sequences (a duplicate) and fence its
   * old-epoch requests. New sends wait here first, so a steady load cannot starve the bump.
   */
  async function bumpEpochIfRequested({ signal }: AcquireOptions = {}): Promise<void> {
    while (isEpochBumpDue()) {
      if (gateHolders > 0) {
        await rejectOnAbort(new Promise<void>((resolve) => drainWaiters.add(resolve)), signal);
        continue;
      }
      epochBump ??= initProducerId().finally(() => {
        epochBump = undefined;
      });
      await rejectOnAbort(epochBump, signal);
    }
  }

  function partitionGate(key: string): Lock {
    let gate = partitionGates.get(key);
    if (!gate) {
      gate = new Lock({ timeout: Number.POSITIVE_INFINITY, description: `idempotent produce ${key}` });
      partitionGates.set(key, gate);
    }
    return gate;
  }

  /** Gates in one global order (of their keys), so two sends sharing partitions cannot deadlock. */
  function gatesInOrder(records: LeasedRecords): Lock[] {
    const keys: string[] = [];
    for (const [topic, recordsByPartition] of records) {
      for (const partition of recordsByPartition.keys()) keys.push(partitionKey(topic, partition));
    }
    return keys.sort().map(partitionGate);
  }

  function nextSequence(firstSequence: number, recordCount: number, key: string): number {
    const sequence = firstSequence + recordCount;
    // Sequence is an Int32; rotate only after the last legal value (`2^31-1`) has been used.
    if (sequence > INT_32_MAX_VALUE) {
      logger.debug(`Sequence for ${key} exceeds max value (${sequence}). Rotating to 0.`);
      return SEQUENCE_START;
    }
    return sequence;
  }

  // Declared once per ledger, so each send allocates a single lease object. A lease updates the
  // ledger's `nextSequences`, `gateHolders` and `drainWaiters`, and may request an epoch bump.
  class Lease implements SequenceLease {
    readonly #records: LeasedRecords;
    #gates: Lock[] | undefined;
    #holdsGates = false;
    #identityVersion = identityVersion;
    /** Sequences handed to this send, reused by its retries: a failed attempt may still have been written. */
    #firstSequences: Map<string, number> | undefined;
    #maybeWritten: Set<string> | undefined;

    constructor(records: LeasedRecords) {
      this.#records = records;
    }

    acquire(deadline?: Deadline): Promise<void> | undefined {
      if (this.#holdsGates || !isInitialized()) return undefined;
      this.#gates ??= gatesInOrder(this.#records);
      if (!isEpochBumpDue() && tryAcquireGates(this.#gates)) {
        this.#holdGates();
        return undefined;
      }
      return this.#waitForGates(this.#gates, deadline?.signal);
    }

    async #waitForGates(gates: readonly Lock[], signal: AbortSignal | undefined): Promise<void> {
      await bumpEpochIfRequested({ signal });
      await acquireGates(gates, signal);
      // A bump requested while this send was queued on a gate (by the send it waited for) must
      // run before this one takes sequences that the bump would invalidate.
      while (isEpochBumpDue()) {
        releaseGates(gates);
        await bumpEpochIfRequested({ signal });
        await acquireGates(gates, signal);
      }
      this.#holdGates();
    }

    #holdGates(): void {
      gateHolders += 1;
      this.#holdsGates = true;
    }

    firstSequence(topic: string, partition: number): number | undefined {
      if (!this.#holdsGates) return undefined;
      if (this.#identityVersion !== identityVersion) {
        this.#forgetSequences();
        this.#identityVersion = identityVersion;
      }

      const key = partitionKey(topic, partition);
      this.#firstSequences ??= new Map();
      let firstSequence = this.#firstSequences.get(key);
      if (firstSequence === undefined) {
        firstSequence = nextSequences.get(key) ?? SEQUENCE_START;
        const recordCount = this.#records.get(topic)?.get(partition)?.length ?? 0;
        nextSequences.set(key, nextSequence(firstSequence, recordCount, key));
        this.#firstSequences.set(key, firstSequence);
      }
      return firstSequence;
    }

    markMaybeWritten(partitionKeys: Iterable<string>): void {
      if (!this.#holdsGates) return;
      this.#maybeWritten ??= new Set();
      for (const key of partitionKeys) this.#maybeWritten.add(key);
    }

    retryUnderNewEpoch(): boolean {
      if (!isInitialized()) return false;
      requestEpochBump();
      if (!canBumpEpoch()) return false;
      // The bump restarts every sequence, so this send's ranges are dropped rather than handed back.
      this.#forgetSequences();
      if (this.#holdsGates) this.#releaseGates();
      return true;
    }

    release(ackedPartitionKeys?: ReadonlySet<string>): void {
      if (!this.#holdsGates) return;
      // After a bump the ranges belong to an older epoch, and the bump already reset them.
      if (ackedPartitionKeys && this.#identityVersion === identityVersion) {
        this.#settleFailedSequences(ackedPartitionKeys);
      }
      this.#releaseGates();
    }

    #settleFailedSequences(ackedPartitionKeys: ReadonlySet<string>): void {
      if (!this.#firstSequences) return;
      if (this.#maybeWritten) {
        for (const key of this.#maybeWritten) {
          if (!ackedPartitionKeys.has(key)) {
            requestEpochBump();
            return;
          }
        }
      }
      // The gates are still held, so each unacked range is the last one handed out on its partition.
      for (const [key, firstSequence] of this.#firstSequences) {
        if (!ackedPartitionKeys.has(key)) nextSequences.set(key, firstSequence);
      }
    }

    #forgetSequences(): void {
      this.#firstSequences?.clear();
      this.#maybeWritten?.clear();
    }

    #releaseGates(): void {
      this.#holdsGates = false;
      if (this.#gates) releaseGates(this.#gates);
      gateHolders -= 1;
      if (gateHolders === 0) {
        for (const notify of drainWaiters) notify();
        drainWaiters.clear();
      }
    }
  }

  return {
    getProducerId: () => producerId,
    getProducerEpoch: () => producerEpoch,
    isInitialized,
    initProducerId,
    requestEpochBump,
    isEpochBumpRequested: () => epochBumpRequested,
    bumpEpochIfRequested,
    lease: (records) => new Lease(records),
  };
}
