import { describe, expect, it } from 'vitest';
import { CreateTopics } from './index';

describe('protocol/requests/create-topics', () => {
  it('implements versions 2 through 3', () => {
    expect(CreateTopics.versions).toEqual([2, 3]);
  });
});
