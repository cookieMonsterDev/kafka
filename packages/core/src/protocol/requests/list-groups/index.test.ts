import { describe, expect, it } from 'vitest';
import { ListGroups } from './index';

describe('protocol/requests/list-groups', () => {
  it('implements versions 0 through 2', () => {
    expect(ListGroups.versions).toEqual([0, 1, 2]);
  });
});
