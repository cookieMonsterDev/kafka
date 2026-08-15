import { describe, expect, it } from 'vitest';
import { groupBy } from './group-by.js';

describe('utils/groupBy', () => {
  it('groups items by the function return', async () => {
    const output = new Map([
      ['even', [2, 4]],
      ['odd', [1, 3]],
    ]);

    await expect(groupBy([1, 2, 3, 4], (item) => (item % 2 === 0 ? 'even' : 'odd'))).resolves.toEqual(output);
  });

  it('works with async group functions', async () => {
    const output = new Map([
      ['even', [2, 4]],
      ['odd', [1, 3]],
    ]);

    await expect(groupBy([1, 2, 3, 4], async (item) => (item % 2 === 0 ? 'even' : 'odd'))).resolves.toEqual(output);
  });

  it('works with object references as group keys', async () => {
    const even = {};
    const odd = {};

    const output = new Map([
      [even, [2, 4]],
      [odd, [1, 3]],
    ]);

    await expect(groupBy([1, 2, 3, 4], async (item) => (item % 2 === 0 ? even : odd))).resolves.toEqual(output);
  });
});
