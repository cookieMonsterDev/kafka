import { describe, expect, it } from 'vitest';
import { Produce } from './index';

describe('protocol/requests/produce', () => {
  it('implements versions 3 through 7 (v0-v2 removed in Kafka 4.0)', () => {
    expect(Produce.versions).toEqual([3, 4, 5, 6, 7]);
  });
});
