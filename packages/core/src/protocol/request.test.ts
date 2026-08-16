import { describe, expect, it } from 'vitest';
import { Decoder } from './decoder';
import { Encoder } from './encoder';
import { createRequest } from './request';

describe('protocol/createRequest', () => {
  it('wraps the encoded body with a length prefix and the request header', async () => {
    const body = new Encoder().writeString('topic-name').writeInt32(42);
    const request = {
      apiKey: 3,
      apiVersion: 6,
      encode: () => Promise.resolve(body),
    };

    const encoded = await createRequest({ correlationId: 1234, clientId: 'my-client', request });
    const decoder = new Decoder(encoded.buffer);

    const size = decoder.readInt32();
    expect(size).toBe(encoded.buffer.length - 4);

    expect(decoder.readInt16()).toBe(3); // apiKey
    expect(decoder.readInt16()).toBe(6); // apiVersion
    expect(decoder.readInt32()).toBe(1234); // correlationId
    expect(decoder.readString()).toBe('my-client'); // clientId
    expect(decoder.readString()).toBe('topic-name');
    expect(decoder.readInt32()).toBe(42);
  });

  it('awaits the request body before framing it', async () => {
    const body = new Encoder().writeInt8(9);
    const request = {
      apiKey: 18,
      apiVersion: 0,
      encode: () => Promise.resolve(body),
    };

    const encoded = await createRequest({ correlationId: 0, clientId: '', request });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32(); // size
    decoder.readInt16(); // apiKey
    decoder.readInt16(); // apiVersion
    decoder.readInt32(); // correlationId
    expect(decoder.readString()).toBe(''); // clientId
    expect(decoder.readInt8()).toBe(9);
  });

  it('encodes different api key/version/correlationId combinations correctly', async () => {
    const request = {
      apiKey: -1,
      apiVersion: 11,
      encode: () => Promise.resolve(new Encoder()),
    };

    const encoded = await createRequest({ correlationId: 2147483647, clientId: 'x', request });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32(); // size

    expect(decoder.readInt16()).toBe(-1);
    expect(decoder.readInt16()).toBe(11);
    expect(decoder.readInt32()).toBe(2147483647);
    expect(decoder.readString()).toBe('x');
  });
});
