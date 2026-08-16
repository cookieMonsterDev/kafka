import { describe, expect, it } from 'vitest';
import { DeleteTopics } from './index';

describe('protocol/requests/delete-topics', () => {
  it('implements versions 0 through 1', () => {
    expect(DeleteTopics.versions).toEqual([0, 1]);
  });
});
