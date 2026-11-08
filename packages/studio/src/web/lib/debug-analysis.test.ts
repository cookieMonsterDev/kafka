import { describe, expect, it } from 'vitest';
import type { DebugRecord } from '../../shared/contracts/debug';
import { compactedState, lagDiagnostics, sampledKeys } from './debug-analysis';

const record = (offset: string, partition: number, value: string | null): DebugRecord => ({
  topic: 'state',
  key: 'aw==',
  value,
  offset,
  partition,
  timestamp: '0',
  headers: {},
  size: 1,
});
const partition = (committedOffset: string | null, logEndOffset: string, lag: string | null) => ({
  topic: 'orders',
  partition: 0,
  committedOffset,
  logEndOffset,
  lag,
});
describe('debugger analysis', () => {
  it('keeps tombstones, bigint offset order, partition-local histories, and omits null keys', () => {
    const state = compactedState([
      record('9007199254740993', 0, null),
      record('9007199254740992', 0, 'dg=='),
      record('0', 1, 'dg=='),
      { ...record('1', 0, null), key: null },
    ]);
    expect(state).toHaveLength(2);
    expect(state[0]?.tombstone).toBe(true);
    expect(state[0]?.history.map((entry) => entry.offset)).toEqual(['9007199254740992', '9007199254740993']);
  });
  it('estimates catchup only for positive net commit progress', () => {
    const samples = [
      { time: 0, partitions: [partition('100', '200', '100')] },
      { time: 10000, partitions: [partition('150', '210', '60')] },
    ];
    expect(lagDiagnostics(samples)[0]).toMatchObject({ committedRate: 5, incomingRate: 1, catchUpSeconds: 15 });
  });
  it('does not invent rates for offset resets or unknown commits', () => {
    expect(
      lagDiagnostics([
        { time: 0, partitions: [partition('100', '200', '100')] },
        { time: 10000, partitions: [partition('50', '210', '160')] },
      ])[0],
    ).toMatchObject({ reset: true, catchUpSeconds: null });
    expect(
      lagDiagnostics([
        { time: 0, partitions: [partition(null, '200', null)] },
        { time: 10000, partitions: [partition(null, '210', null)] },
      ])[0]?.committedRate,
    ).toBeNull();
  });
});

describe('lag review regressions', () => {
  it('detects an intervening reset even when the final offset is ahead', () => {
    expect(
      lagDiagnostics([
        { time: 0, partitions: [partition('100', '200', '100')] },
        { time: 5000, partitions: [partition('0', '200', '200')] },
        { time: 10000, partitions: [partition('150', '210', '60')] },
      ])[0],
    ).toMatchObject({ reset: true, committedRate: null, catchUpSeconds: null });
  });
  it('detects a 30-second stall after earlier progress in the retained window', () => {
    expect(
      lagDiagnostics([
        { time: 0, partitions: [partition('100', '200', '100')] },
        { time: 5000, partitions: [partition('150', '210', '60')] },
        { time: 35000, partitions: [partition('150', '210', '60')] },
      ])[0]?.stalled,
    ).toBe(true);
  });
});

it('keeps distinct binary and null keys in the sampled hot-key histogram', () => {
  const keys = sampledKeys(['/w==', '/g==', null, 'bnVsbA=='].map((key) => ({ ...record('0', 0, null), key })));
  expect(keys).toHaveLength(4);
  expect(keys.every((entry) => entry.count === 1)).toBe(true);
});
