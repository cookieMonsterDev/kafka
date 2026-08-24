import { describe, expect, it } from 'vitest';
import { produceResponseV9 } from '../v9/response';
import { produceResponseV10 } from './response';

describe('protocol/requests/produce/v10/response', () => {
  it('re-exports the v9 decoder', () => {
    expect(produceResponseV10).toBe(produceResponseV9);
  });
});
