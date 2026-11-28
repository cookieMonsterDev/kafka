import { describe, expect, it } from 'vitest';
import { listOffsetsResponseV6 } from '../v6/response';
import { listOffsetsResponseV9 } from './response';

describe('protocol/requests/list-offsets/v9/response', () => {
  it('re-exports the v6 decoder', () => {
    expect(listOffsetsResponseV9).toBe(listOffsetsResponseV6);
  });
});
