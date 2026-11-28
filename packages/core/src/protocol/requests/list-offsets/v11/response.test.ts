import { describe, expect, it } from 'vitest';
import { listOffsetsResponseV6 } from '../v6/response';
import { listOffsetsResponseV11 } from './response';

describe('protocol/requests/list-offsets/v11/response', () => {
  it('re-exports the v6 decoder', () => {
    expect(listOffsetsResponseV11).toBe(listOffsetsResponseV6);
  });
});
