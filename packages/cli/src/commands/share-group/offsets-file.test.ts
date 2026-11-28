import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { parseShareGroupOffsetsFile, parseStartOffsetEntry } from './offsets-file';

const OPTIONS = { path: 'offsets.csv', groupId: 'sg1' };
const SET_CONTEXT = { source: '--set', entry: 'raw' };

describe('parseStartOffsetEntry', () => {
  it('parses a topic, a partition, and a bigint offset', () => {
    expect(parseStartOffsetEntry(['orders', '1', '9007199254740993'], SET_CONTEXT)).toEqual({
      topic: 'orders',
      partition: 1,
      offset: 9007199254740993n,
    });
  });

  it('accepts the INT32 partition and INT64 offset bounds', () => {
    expect(parseStartOffsetEntry(['orders', '2147483647', '-9223372036854775808'], SET_CONTEXT)).toEqual({
      topic: 'orders',
      partition: 2147483647,
      offset: -9223372036854775808n,
    });
    expect(parseStartOffsetEntry(['orders', '0', '9223372036854775807'], SET_CONTEXT).offset).toBe(
      9223372036854775807n,
    );
  });

  it.each([
    [['', '0', '1'], /topic must not be empty/],
    [['orders', '-1', '1'], /partition must be an integer from 0 to 2147483647/],
    [['orders', 'x', '1'], /partition must be an integer from 0 to 2147483647/],
    [['orders', '2147483648', '1'], /partition must be an integer from 0 to 2147483647/],
    [['orders', '0', ''], /offset must be a 64-bit integer/],
    [['orders', '0', '0x10'], /offset must be a 64-bit integer/],
    [['orders', '0', '9223372036854775808'], /offset must be a 64-bit integer/],
    [['orders', '0', '-9223372036854775809'], /offset must be a 64-bit integer/],
  ] as const)('rejects %j', (fields, message) => {
    expect(() => parseStartOffsetEntry(fields, SET_CONTEXT)).toThrow(message);
  });

  it('prefixes errors with where the entry came from and quotes the raw entry', () => {
    expect(() => parseStartOffsetEntry(['orders', 'x', '1'], { source: '--set', entry: 'orders:x:1' })).toThrow(
      '--set partition must be an integer from 0 to 2147483647, got "orders:x:1"',
    );
  });
});

describe('parseShareGroupOffsetsFile', () => {
  it('reads topic,partition,offset records with no header', () => {
    expect(parseShareGroupOffsetsFile('orders,0,100\norders,1,200\npayments,0,0\n', OPTIONS)).toEqual([
      { topic: 'orders', partition: 0, offset: 100n },
      { topic: 'orders', partition: 1, offset: 200n },
      { topic: 'payments', partition: 0, offset: 0n },
    ]);
  });

  it('accepts CRLF line endings and skips blank lines', () => {
    expect(parseShareGroupOffsetsFile('\r\norders,0,100\r\n\r\norders,1,200\r\n', OPTIONS)).toEqual([
      { topic: 'orders', partition: 0, offset: 100n },
      { topic: 'orders', partition: 1, offset: 200n },
    ]);
  });

  it('keeps only the share group’s records from a group,topic,partition,offset file', () => {
    const text = 'other,orders,0,5\nsg1,orders,0,100\nsg1,payments,2,7\n';
    expect(parseShareGroupOffsetsFile(text, OPTIONS)).toEqual([
      { topic: 'orders', partition: 0, offset: 100n },
      { topic: 'payments', partition: 2, offset: 7n },
    ]);
  });

  it('unquotes a double-quoted group id, including an escaped quote', () => {
    const text = '"sg 1, ""blue""",orders,0,100\n';
    expect(parseShareGroupOffsetsFile(text, { ...OPTIONS, groupId: 'sg 1, "blue"' })).toEqual([
      { topic: 'orders', partition: 0, offset: 100n },
    ]);
  });

  it('keeps the last offset for a repeated topic partition', () => {
    expect(parseShareGroupOffsetsFile('orders,0,100\norders,0,300\n', OPTIONS)).toEqual([
      { topic: 'orders', partition: 0, offset: 300n },
    ]);
  });

  it('rejects a file with no records', () => {
    expect(() => parseShareGroupOffsetsFile('\n\n', OPTIONS)).toThrow('--from-file "offsets.csv" has no entries');
  });

  it('rejects a group file with no records for the share group', () => {
    expect(() => parseShareGroupOffsetsFile('other,orders,0,5\n', OPTIONS)).toThrow(
      '--from-file "offsets.csv" has no entries for share group "sg1"',
    );
  });

  it('rejects a first record that is neither format', () => {
    expect(() => parseShareGroupOffsetsFile('orders:0:100\n', OPTIONS)).toThrow(
      '--from-file "offsets.csv" line 1: expected "topic,partition,offset" or "group,topic,partition,offset", got "orders:0:100"',
    );
  });

  it('rejects a record whose field count differs from the first record', () => {
    expect(() => parseShareGroupOffsetsFile('orders,0,100\nsg1,orders,1,200\n', OPTIONS)).toThrow(
      '--from-file "offsets.csv" line 2: expected "topic,partition,offset" like the first line, got "sg1,orders,1,200"',
    );
  });

  it('validates records of other groups too, as Kafka parses every record', () => {
    expect(() => parseShareGroupOffsetsFile('other,orders,x,5\nsg1,orders,0,100\n', OPTIONS)).toThrow(
      /line 1: partition must be an integer from 0 to 2147483647/,
    );
  });

  it('names the line of an invalid offset', () => {
    expect(() => parseShareGroupOffsetsFile('orders,0,100\norders,1,abc\n', OPTIONS)).toThrow(
      '--from-file "offsets.csv" line 2: offset must be a 64-bit integer, got "orders,1,abc"',
    );
  });

  it.each(['"orders,0,100', '"orders"x,0,100'])('rejects malformed quoting in %j', (line) => {
    expect(() => parseShareGroupOffsetsFile(`${line}\n`, OPTIONS)).toThrow(CliUsageError);
  });
});
