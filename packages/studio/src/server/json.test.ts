import type { IncomingMessage } from 'node:http';
import { EventEmitter } from 'node:events';
import { describe, expect, it } from 'vitest';
import { UnknownProfileError } from './kafka/connection';
import {
  InvalidJsonBodyError,
  mapErrorToApiError,
  readJsonBody,
  RequestBodyTooLargeError,
  stringifyJson,
} from './json';

function fakeRequest(chunks: string[]): IncomingMessage {
  const emitter = new EventEmitter() as EventEmitter & { destroy(): void };
  emitter.destroy = () => {
    emitter.removeAllListeners();
  };
  queueMicrotask(() => {
    for (const chunk of chunks) emitter.emit('data', Buffer.from(chunk));
    emitter.emit('end');
  });
  return emitter as unknown as IncomingMessage;
}

describe('stringifyJson', () => {
  it('renders a bigint as its decimal string', () => {
    expect(stringifyJson({ offset: 42n })).toBe('{"offset":"42"}');
  });

  it('behaves like JSON.stringify for ordinary values', () => {
    expect(stringifyJson({ a: 1, b: 'two', c: null })).toBe(JSON.stringify({ a: 1, b: 'two', c: null }));
  });
});

describe('readJsonBody', () => {
  it('parses a JSON body split across chunks', async () => {
    await expect(readJsonBody(fakeRequest(['{"a":', '1}']))).resolves.toEqual({ a: 1 });
  });

  it('resolves undefined for an empty body', async () => {
    await expect(readJsonBody(fakeRequest(['']))).resolves.toBeUndefined();
  });

  it('rejects with InvalidJsonBodyError for malformed JSON', async () => {
    await expect(readJsonBody(fakeRequest(['{not json']))).rejects.toBeInstanceOf(InvalidJsonBodyError);
  });

  it('rejects with RequestBodyTooLargeError past the limit', async () => {
    await expect(readJsonBody(fakeRequest(['{"a": 1}']), 2)).rejects.toBeInstanceOf(RequestBodyTooLargeError);
  });
});

describe('mapErrorToApiError', () => {
  it('maps InvalidJsonBodyError to a 400 bad_request', () => {
    expect(mapErrorToApiError(new InvalidJsonBodyError(new Error('nope')))).toMatchObject({
      status: 400,
      code: 'bad_request',
    });
  });

  it('maps an UnknownProfileError to a 404 with the available profiles', () => {
    const error = new UnknownProfileError('x', ['a', 'b']);
    expect(mapErrorToApiError(error)).toEqual({
      status: 404,
      code: 'unknown_profile',
      message: error.message,
      details: { available: ['a', 'b'] },
    });
  });

  it('falls back to a 500 internal_error for anything unrecognized', () => {
    expect(mapErrorToApiError(new Error('kaboom'))).toEqual({
      status: 500,
      code: 'internal_error',
      message: 'kaboom',
    });
  });

  it('falls back gracefully for a non-Error throw', () => {
    expect(mapErrorToApiError('kaboom')).toEqual({
      status: 500,
      code: 'internal_error',
      message: 'unexpected error',
    });
  });
});
