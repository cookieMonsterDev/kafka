import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { parseElectionFile } from './election-file';

describe('parseElectionFile', () => {
  it('parses the kafka-leader-election.sh --path-to-json-file shape', () => {
    const parsed = parseElectionFile({
      partitions: [
        { topic: 'foo', partition: 1 },
        { topic: 'foobar', partition: 2 },
      ],
    });

    expect(parsed).toEqual([
      { topic: 'foo', partitions: [1] },
      { topic: 'foobar', partitions: [2] },
    ]);
  });

  it('groups multiple partitions for the same topic', () => {
    const parsed = parseElectionFile({
      partitions: [
        { topic: 'foo', partition: 0 },
        { topic: 'foo', partition: 1 },
      ],
    });

    expect(parsed).toEqual([{ topic: 'foo', partitions: [0, 1] }]);
  });

  it('de-duplicates a repeated topic/partition pair', () => {
    const parsed = parseElectionFile({
      partitions: [
        { topic: 'foo', partition: 0 },
        { topic: 'foo', partition: 0 },
      ],
    });

    expect(parsed).toEqual([{ topic: 'foo', partitions: [0] }]);
  });

  it('rejects a non-object payload', () => {
    expect(() => parseElectionFile([])).toThrow(CliUsageError);
    expect(() => parseElectionFile(null)).toThrow(CliUsageError);
  });

  it('rejects a payload with no "partitions" array', () => {
    expect(() => parseElectionFile({})).toThrow(/"partitions" array/);
  });

  it('rejects an empty "partitions" array', () => {
    expect(() => parseElectionFile({ partitions: [] })).toThrow(/lists no partitions/);
  });

  it('rejects an entry missing "topic"', () => {
    expect(() => parseElectionFile({ partitions: [{ partition: 1 }] })).toThrow(/"topic"/);
  });

  it('rejects an entry with a negative "partition"', () => {
    expect(() => parseElectionFile({ partitions: [{ topic: 'foo', partition: -1 }] })).toThrow(/"partition"/);
  });

  it('rejects an entry with a non-integer "partition"', () => {
    expect(() => parseElectionFile({ partitions: [{ topic: 'foo', partition: 1.5 }] })).toThrow(CliUsageError);
  });
});
