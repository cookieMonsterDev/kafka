import { describe, expect, it } from 'vitest';
import { OffsetFetch } from './index.js';

describe('protocol/requests/offset-fetch', () => {
  it('implements versions 1 through 4 — kafkajs never had a v0 here', () => {
    expect(OffsetFetch.versions).toEqual([1, 2, 3, 4]);
  });
});
