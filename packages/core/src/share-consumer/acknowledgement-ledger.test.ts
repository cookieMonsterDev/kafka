import { describe, expect, it } from 'vitest';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
import { AcknowledgementLedger, toAcknowledgeEventTopics } from './acknowledgement-ledger';

const { ACCEPT, RELEASE, REJECT } = SHARE_ACKNOWLEDGE_TYPE;

function range(firstOffset: bigint, lastOffset = firstOffset) {
  return { firstOffset, lastOffset, deliveryCount: 1 };
}

describe('share-consumer/acknowledgement-ledger', () => {
  it("takes a partition's queued acknowledgements in order, once", () => {
    const ledger = new AcknowledgementLedger();
    ledger.queue('events', 0, [range(0n, 4n), range(7n)], ACCEPT);
    ledger.queue('events', 0, [range(9n)], RELEASE);
    ledger.queue('events', 1, [range(3n)], REJECT);

    expect(ledger.take('events', 0)).toEqual([
      { firstOffset: 0n, lastOffset: 4n, acknowledgeType: ACCEPT },
      { firstOffset: 7n, lastOffset: 7n, acknowledgeType: ACCEPT },
      { firstOffset: 9n, lastOffset: 9n, acknowledgeType: RELEASE },
    ]);
    expect(ledger.take('events', 0)).toBeUndefined();
    expect(ledger.take('events', 1)).toEqual([{ firstOffset: 3n, lastOffset: 3n, acknowledgeType: REJECT }]);
    expect(ledger.take('clicks', 0)).toBeUndefined();
  });

  it('queues nothing for a batch without acquired records', () => {
    const ledger = new AcknowledgementLedger();
    ledger.queue('events', 0, [], ACCEPT);

    expect(ledger.take('events', 0)).toBeUndefined();
  });

  it('restores taken acknowledgements ahead of those queued since', () => {
    const ledger = new AcknowledgementLedger();
    ledger.queue('events', 0, [range(1n)], ACCEPT);
    const acknowledgements = ledger.take('events', 0) ?? [];
    ledger.queue('events', 0, [range(2n)], ACCEPT);

    ledger.restore([{ topic: 'events', partition: 0, acknowledgements }]);

    expect(ledger.take('events', 0)?.map(({ firstOffset }) => firstOffset)).toEqual([1n, 2n]);
  });

  it('clears every pending acknowledgement', () => {
    const ledger = new AcknowledgementLedger();
    ledger.queue('events', 0, [range(1n)], ACCEPT);
    ledger.queue('clicks', 2, [range(1n)], ACCEPT);
    ledger.clear();

    expect(ledger.take('events', 0)).toBeUndefined();
    expect(ledger.take('clicks', 2)).toBeUndefined();
  });

  it('groups taken acknowledgements by topic for the ACKNOWLEDGE event', () => {
    expect(
      toAcknowledgeEventTopics([
        {
          topic: 'events',
          partition: 0,
          acknowledgements: [{ firstOffset: 1n, lastOffset: 2n, acknowledgeType: ACCEPT }],
        },
        {
          topic: 'clicks',
          partition: 3,
          acknowledgements: [{ firstOffset: 5n, lastOffset: 5n, acknowledgeType: REJECT }],
        },
        {
          topic: 'events',
          partition: 1,
          acknowledgements: [{ firstOffset: 4n, lastOffset: 4n, acknowledgeType: RELEASE }],
        },
      ]),
    ).toEqual([
      {
        topic: 'events',
        partitions: [
          { partition: 0, firstOffset: 1n, lastOffset: 2n, acknowledgeType: ACCEPT },
          { partition: 1, firstOffset: 4n, lastOffset: 4n, acknowledgeType: RELEASE },
        ],
      },
      { topic: 'clicks', partitions: [{ partition: 3, firstOffset: 5n, lastOffset: 5n, acknowledgeType: REJECT }] },
    ]);
  });
});
