import { describe, expect, it } from 'vitest';
import { listOffsetsResponseV6 } from '../v6/response';
import { listOffsetsResponseV10 } from './response';

describe('protocol/requests/list-offsets/v10/response', () => {
  it('re-exports the v6 decoder', () => {
    expect(listOffsetsResponseV10).toBe(listOffsetsResponseV6);
  });
});
