import { describe, expect, it } from 'vitest';
import { KafkaServerDoesNotSupportApiKey } from '../../../errors';
import { OFFSET_SPECS } from '../../enums/offset-specs';
import { ListOffsets, type ListOffsetsOptions } from './index';

function lookupOf(timestamp: bigint): ListOffsetsOptions {
  return { topics: [{ topic: 't', partitions: [{ partition: 0 }, { partition: 1, timestamp }] }] };
}

describe('protocol/requests/list-offsets', () => {
  it('implements versions 0 through 11', () => {
    expect(ListOffsets.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  });

  it('writes timeoutMs on v10+ only, defaulting to 30000', async () => {
    const topics = [{ topic: 't', partitions: [{ partition: 0 }] }];
    const v9 = await ListOffsets.protocol({ version: 9 })({ topics, timeoutMs: 5000 }).request.encode();
    const v10 = await ListOffsets.protocol({ version: 10 })({ topics, timeoutMs: 5000 }).request.encode();
    expect(v10.buffer.length).toBe(v9.buffer.length + 4);
    expect(v10.buffer.readInt32BE(v10.buffer.length - 5)).toBe(5000);

    const defaulted = await ListOffsets.protocol({ version: 11 })({ topics }).request.encode();
    expect(defaulted.buffer.readInt32BE(defaulted.buffer.length - 5)).toBe(30_000);
  });

  it('waits for a v10+ response a little longer than the broker may wait on tiered storage', () => {
    const topics = [{ topic: 't', partitions: [{ partition: 0 }] }];
    expect(ListOffsets.protocol({ version: 9 })({ topics, timeoutMs: 5000 }).requestTimeout).toBeUndefined();
    expect(ListOffsets.protocol({ version: 10 })({ topics, timeoutMs: 5000 }).requestTimeout).toBe(5100);
    expect(ListOffsets.protocol({ version: 11 })({ topics }).requestTimeout).toBe(30_100);
  });

  it.each([
    [OFFSET_SPECS.MAX_TIMESTAMP, 7],
    [OFFSET_SPECS.EARLIEST_LOCAL, 8],
    [OFFSET_SPECS.LATEST_TIERED, 9],
    [OFFSET_SPECS.EARLIEST_PENDING_UPLOAD, 11],
  ])('sends offset spec %s on v%i and later only', (timestamp, minimumVersion) => {
    const advertised = { minVersion: 0, maxVersion: minimumVersion - 1 };
    expect(ListOffsets.preferVersion?.(lookupOf(timestamp), minimumVersion, advertised)).toBe(minimumVersion);
    expect(() => ListOffsets.preferVersion?.(lookupOf(timestamp), minimumVersion - 1, advertised)).toThrow(
      KafkaServerDoesNotSupportApiKey,
    );
    expect(() => ListOffsets.preferVersion?.(lookupOf(timestamp), minimumVersion - 1, advertised)).toThrow(
      `ListOffsets v${minimumVersion}+`,
    );
  });

  it('sends latest, earliest, and real timestamps on any version', () => {
    const advertised = { minVersion: 0, maxVersion: 0 };
    for (const timestamp of [OFFSET_SPECS.LATEST, OFFSET_SPECS.EARLIEST, 1_735_689_600_000n]) {
      expect(ListOffsets.preferVersion?.(lookupOf(timestamp), 0, advertised)).toBe(0);
    }
  });

  it('defaults currentLeaderEpoch to -1 on v4+ and does not write it on v3', async () => {
    const topics = [{ topic: 't', partitions: [{ partition: 0, timestamp: 1n, currentLeaderEpoch: 9 }] }];
    const v3 = await ListOffsets.protocol({ version: 3 })({
      replicaId: -1,
      isolationLevel: 0,
      topics,
    }).request.encode();
    const v4 = await ListOffsets.protocol({ version: 4 })({
      replicaId: -1,
      isolationLevel: 0,
      topics,
    }).request.encode();
    expect(v4.buffer.length).toBe(v3.buffer.length + 4);

    const omitted = await ListOffsets.protocol({ version: 4 })({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 't', partitions: [{ partition: 0, timestamp: 1n }] }],
    }).request.encode();
    const explicitUnknown = await ListOffsets.protocol({ version: 4 })({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 't', partitions: [{ partition: 0, timestamp: 1n, currentLeaderEpoch: -1 }] }],
    }).request.encode();
    expect(omitted.buffer).toEqual(explicitUnknown.buffer);
  });
});
