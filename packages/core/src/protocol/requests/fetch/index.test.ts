import { describe, expect, it } from 'vitest';
import { Fetch } from './index';

describe('protocol/requests/fetch', () => {
  it('implements versions 4 through 11 (v0-v3 removed in Kafka 4.0)', () => {
    expect(Fetch.versions).toEqual([4, 5, 6, 7, 8, 9, 10, 11]);
  });
});
