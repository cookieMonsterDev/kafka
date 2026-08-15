import { describe, expect, it } from 'vitest';
import { AlterConfigs } from './index.js';

describe('protocol/requests/alter-configs', () => {
  it('implements versions 0 through 1', () => {
    expect(AlterConfigs.versions).toEqual([0, 1]);
  });
});
