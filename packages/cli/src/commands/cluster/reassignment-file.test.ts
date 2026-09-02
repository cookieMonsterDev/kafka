import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { parseReassignmentFile } from './reassignment-file';

describe('parseReassignmentFile', () => {
  it('parses the kafka-reassign-partitions.sh --reassignment-json-file shape', () => {
    const parsed = parseReassignmentFile({
      partitions: [{ topic: 'foo', partition: 1, replicas: [1, 2, 3] }],
      version: 1,
    });

    expect(parsed).toEqual({
      topics: [{ topic: 'foo', partitionAssignment: [{ partition: 1, replicas: [1, 2, 3] }] }],
      hasLogDirs: false,
    });
  });

  it('accepts a valid log_dirs entry and reports hasLogDirs', () => {
    const parsed = parseReassignmentFile({
      partitions: [{ topic: 'foo', partition: 1, replicas: [1, 2, 3], log_dirs: ['any', '/data/1', 'any'] }],
    });

    expect(parsed.hasLogDirs).toBe(true);
    expect(parsed.topics).toEqual([{ topic: 'foo', partitionAssignment: [{ partition: 1, replicas: [1, 2, 3] }] }]);
  });

  it('groups multiple partitions for the same topic under one entry', () => {
    const parsed = parseReassignmentFile({
      partitions: [
        { topic: 'foo', partition: 0, replicas: [1, 2] },
        { topic: 'foo', partition: 1, replicas: [2, 3] },
      ],
    });

    expect(parsed.topics).toEqual([
      {
        topic: 'foo',
        partitionAssignment: [
          { partition: 0, replicas: [1, 2] },
          { partition: 1, replicas: [2, 3] },
        ],
      },
    ]);
  });

  it('rejects a non-object payload', () => {
    expect(() => parseReassignmentFile([])).toThrow(CliUsageError);
    expect(() => parseReassignmentFile(null)).toThrow(CliUsageError);
    expect(() => parseReassignmentFile('nope')).toThrow(CliUsageError);
  });

  it('rejects a payload with no "partitions" array', () => {
    expect(() => parseReassignmentFile({})).toThrow(/"partitions" array/);
  });

  it('rejects an empty "partitions" array', () => {
    expect(() => parseReassignmentFile({ partitions: [] })).toThrow(/lists no partitions/);
  });

  it('rejects an entry missing "topic"', () => {
    expect(() => parseReassignmentFile({ partitions: [{ partition: 1, replicas: [1] }] })).toThrow(/"topic"/);
  });

  it('rejects an entry with a non-string "topic"', () => {
    expect(() => parseReassignmentFile({ partitions: [{ topic: 5, partition: 1, replicas: [1] }] })).toThrow(
      CliUsageError,
    );
  });

  it('rejects an entry with a negative "partition"', () => {
    expect(() => parseReassignmentFile({ partitions: [{ topic: 'foo', partition: -1, replicas: [1] }] })).toThrow(
      /"partition"/,
    );
  });

  it('rejects an entry with a non-integer "partition"', () => {
    expect(() => parseReassignmentFile({ partitions: [{ topic: 'foo', partition: 1.5, replicas: [1] }] })).toThrow(
      CliUsageError,
    );
  });

  it('rejects an entry with an empty "replicas" array', () => {
    expect(() => parseReassignmentFile({ partitions: [{ topic: 'foo', partition: 0, replicas: [] }] })).toThrow(
      /"replicas"/,
    );
  });

  it('rejects an entry with a negative replica id', () => {
    expect(() => parseReassignmentFile({ partitions: [{ topic: 'foo', partition: 0, replicas: [1, -2] }] })).toThrow(
      /non-negative integers/,
    );
  });

  it('rejects log_dirs whose length does not match replicas', () => {
    expect(() =>
      parseReassignmentFile({ partitions: [{ topic: 'foo', partition: 0, replicas: [1, 2], log_dirs: ['any'] }] }),
    ).toThrow(/one entry per replica/);
  });

  it('rejects a log_dirs entry that is neither "any" nor an absolute path', () => {
    expect(() =>
      parseReassignmentFile({
        partitions: [{ topic: 'foo', partition: 0, replicas: [1], log_dirs: ['relative/path'] }],
      }),
    ).toThrow(/"any" or an absolute path/);
  });
});
