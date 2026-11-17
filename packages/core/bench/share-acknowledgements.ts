/**
 * Share-consumer acknowledgement path, without a broker: handlers queue one acknowledgement per
 * batch, then each ShareFetch round takes every partition's pending acknowledgements and turns them
 * into request batches. `flat-list scan` is the baseline: the list ShareRunner kept before
 * `AcknowledgementLedger`, scanned once per partition on every fetch.
 */
import type { ShareFetchAcknowledgementBatchInput } from '../src/protocol/requests/share-fetch/index';
import { SHARE_ACKNOWLEDGE_TYPE, type ShareAcknowledgeType } from '../src/share-consumer/acknowledge-types';
import {
  AcknowledgementLedger,
  toAcknowledgementBatches,
  type PendingAcknowledgement,
} from '../src/share-consumer/acknowledgement-ledger';
import { measure, printStats, type BenchStats } from './measure';

const PARTITIONS = Number(process.env.BENCH_SHARE_PARTITIONS ?? 64);
const BATCHES_PER_PARTITION = Number(process.env.BENCH_SHARE_BATCHES ?? 20);
const ROUNDS = Number(process.env.BENCH_SHARE_ROUNDS ?? 50);
const ITERATIONS = Number(process.env.BENCH_SHARE_ITERATIONS ?? 10);
const TOPIC = 'events';
const ACKNOWLEDGEMENTS_PER_ITERATION = PARTITIONS * BATCHES_PER_PARTITION * ROUNDS;

interface FlatPendingAcknowledgement extends PendingAcknowledgement {
  topic: string;
  partition: number;
}

function acquiredRange(offset: bigint) {
  return [{ firstOffset: offset, lastOffset: offset, deliveryCount: 1 }];
}

/** The pre-ledger take: one pass over every pending acknowledgement for each partition. */
function takeFromFlatList(
  pending: FlatPendingAcknowledgement[],
  partition: number,
): { remaining: FlatPendingAcknowledgement[]; batches: ShareFetchAcknowledgementBatchInput[] } {
  const remaining: FlatPendingAcknowledgement[] = [];
  const batches: ShareFetchAcknowledgementBatchInput[] = [];
  for (const acknowledgement of pending) {
    if (acknowledgement.topic === TOPIC && acknowledgement.partition === partition) {
      batches.push({
        firstOffset: acknowledgement.firstOffset,
        lastOffset: acknowledgement.lastOffset,
        acknowledgeTypes: [acknowledgement.acknowledgeType],
      });
    } else {
      remaining.push(acknowledgement);
    }
  }
  return { remaining, batches };
}

export async function benchShareAcknowledgements(): Promise<BenchStats[]> {
  const acknowledgeType: ShareAcknowledgeType = SHARE_ACKNOWLEDGE_TYPE.ACCEPT;
  const shape = `${PARTITIONS} partitions x ${BATCHES_PER_PARTITION} batches x ${ROUNDS} fetch rounds`;
  let sentBatches = 0;

  const flat = await measure({
    name: `share acknowledgements, flat-list scan (${shape})`,
    iterations: ITERATIONS,
    messagesPerIter: ACKNOWLEDGEMENTS_PER_ITERATION,
    bytesPerIter: 0,
    run: () => {
      let pending: FlatPendingAcknowledgement[] = [];
      let offset = 0n;
      for (let round = 0; round < ROUNDS; round++) {
        for (let batch = 0; batch < BATCHES_PER_PARTITION; batch++) {
          for (let partition = 0; partition < PARTITIONS; partition++) {
            for (const { firstOffset, lastOffset } of acquiredRange(offset++)) {
              pending.push({ topic: TOPIC, partition, firstOffset, lastOffset, acknowledgeType });
            }
          }
        }
        for (let partition = 0; partition < PARTITIONS; partition++) {
          const taken = takeFromFlatList(pending, partition);
          pending = taken.remaining;
          sentBatches += taken.batches.length;
        }
      }
    },
  });
  printStats(flat);

  const ledger = await measure({
    name: `share acknowledgements, AcknowledgementLedger (${shape})`,
    iterations: ITERATIONS,
    messagesPerIter: ACKNOWLEDGEMENTS_PER_ITERATION,
    bytesPerIter: 0,
    run: () => {
      const acknowledgements = new AcknowledgementLedger();
      let offset = 0n;
      for (let round = 0; round < ROUNDS; round++) {
        for (let batch = 0; batch < BATCHES_PER_PARTITION; batch++) {
          for (let partition = 0; partition < PARTITIONS; partition++) {
            acknowledgements.queue(TOPIC, partition, acquiredRange(offset++), acknowledgeType);
          }
        }
        for (let partition = 0; partition < PARTITIONS; partition++) {
          const taken = acknowledgements.take(TOPIC, partition);
          if (taken) sentBatches += toAcknowledgementBatches(taken).length;
        }
      }
    },
  });
  printStats(ledger);

  if (sentBatches === 0) throw new Error('no acknowledgements were taken');
  return [flat, ledger];
}
