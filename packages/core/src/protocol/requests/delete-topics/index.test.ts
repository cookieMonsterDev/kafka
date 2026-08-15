import { describe, expect, it } from 'vitest';
import { DeleteTopics } from './index.js';

describe('protocol/requests/delete-topics', () => {
  it('implements version 1 only', () => {
    expect(DeleteTopics.versions).toEqual([1]);
  });
});
