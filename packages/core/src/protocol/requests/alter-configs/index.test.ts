import { describe, expect, it } from 'vitest';
import { AlterConfigs } from './index';

describe('protocol/requests/alter-configs', () => {
  it('implements versions 0 through 2', () => {
    expect(AlterConfigs.versions).toEqual([0, 1, 2]);
  });
});
