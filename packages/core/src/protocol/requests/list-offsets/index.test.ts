import { describe, expect, it } from 'vitest';
import { ListOffsets } from './index.js';

describe('protocol/requests/list-offsets', () => {
  it('implements versions 1 through 3 — v0 is below the real Kafka 4.0.0 floor', () => {
    expect(ListOffsets.versions).toEqual([1, 2, 3]);
  });
});
