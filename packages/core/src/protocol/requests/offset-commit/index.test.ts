import { describe, expect, it } from 'vitest';
import { OffsetCommit } from './index';

describe('protocol/requests/offset-commit', () => {
  it('implements versions 2 through 5 — v0 and v1 are below the real Kafka 4.0.0 floor', () => {
    expect(OffsetCommit.versions).toEqual([2, 3, 4, 5]);
  });
});
