import { describe, expect, it } from 'vitest';
import { decodeProtobuf, encodeProtobuf } from './protobuf-schema';

const definition = {
  schema:
    'syntax = "proto3"; package events; message First { string id = 1; } message Second { message Nested { string id = 1; } string id = 1; }',
};
const noReferences = async () => {
  throw new Error('Unexpected reference');
};
describe('Confluent Protobuf framing', () => {
  it('encodes the first message index using the special zero byte', async () => {
    const encoded = await encodeProtobuf(definition, 7, { id: 'order-1' }, noReferences);
    expect([...encoded.subarray(0, 6)]).toEqual([0, 0, 0, 0, 7, 0]);
    expect(await decodeProtobuf(definition, encoded, noReferences)).toEqual({ id: 'order-1' });
  });
  it('supports non-first and nested message indexes', async () => {
    const second = await encodeProtobuf(definition, 7, { id: 'second' }, noReferences, 'events.Second');
    expect([...second.subarray(5, 7)]).toEqual([2, 2]);
    expect(await decodeProtobuf(definition, second, noReferences)).toEqual({ id: 'second' });
    const nested = await encodeProtobuf(definition, 7, { id: 'nested' }, noReferences, 'events.Second.Nested');
    expect([...nested.subarray(5, 8)]).toEqual([4, 2, 0]);
    expect(await decodeProtobuf(definition, nested, noReferences)).toEqual({ id: 'nested' });
  });
  it('rejects invalid message indexes instead of decoding the wrong type', async () => {
    await expect(decodeProtobuf(definition, Buffer.from([0, 0, 0, 0, 7, 2, 126]), noReferences)).rejects.toThrow(
      'outside',
    );
  });
});

describe('Protobuf JSON fidelity', () => {
  const schema = {
    schema:
      'syntax = "proto3"; message Event { enum Status { UNKNOWN = 0; READY = 1; } uint64 id = 1; int64 signed_id = 2; Status status = 3; bytes data = 4; message Child { int64 value = 1; Status status = 2; } repeated Child children = 5; map<string, uint64> counts = 6; string name = 7; }',
  };
  it('round-trips exact 64-bit boundaries, enum names, bytes, nested messages and maps', async () => {
    const payload = {
      id: '18446744073709551615',
      signedId: '-9223372036854775808',
      status: 'READY',
      data: '/wAB',
      children: [{ value: '9007199254740993', status: 'READY' }],
      counts: { total: '18446744073709551615' },
      name: 'event',
    };
    const encoded = await encodeProtobuf(schema, 1, payload, noReferences);
    const decoded = await decodeProtobuf(schema, encoded, noReferences);
    expect(decoded).toEqual(payload);
    expect(await encodeProtobuf(schema, 1, decoded, noReferences)).toEqual(encoded);
  });
  it.each([
    { id: '18446744073709551616' },
    { id: '-1' },
    { signedId: '-9223372036854775809' },
    { signedId: '9223372036854775808' },
    { id: '1.5' },
    { id: { low: 1, high: Number.MAX_SAFE_INTEGER } },
    { id: 9007199254740992 },
    { status: 'MISSING' },
    { name: 5 },
    { data: 'not-base64!' },
    { unknown: true },
  ])('rejects invalid or lossy JSON forms: %j', async (payload) => {
    await expect(encodeProtobuf(schema, 1, payload, noReferences)).rejects.toThrow();
  });
});
