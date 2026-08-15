import { describe, expect, it } from 'vitest';
import { InitProducerId } from './index.js';

describe('protocol/requests/init-producer-id', () => {
  it('implements versions 0 through 1', () => {
    expect(InitProducerId.versions).toEqual([0, 1]);
  });
});
