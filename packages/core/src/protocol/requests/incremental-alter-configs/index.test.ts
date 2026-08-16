import { describe, expect, it } from 'vitest';
import { IncrementalAlterConfigs } from './index';

describe('protocol/requests/incremental-alter-configs', () => {
  it('implements versions 0 through 1', () => {
    expect(IncrementalAlterConfigs.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const options = {
      resources: [{ type: 2, name: 't', configs: [{ name: 'cleanup.policy', operation: 0, value: 'compact' }] }],
    };
    const { request: v0 } = IncrementalAlterConfigs.protocol({ version: 0 })(options);
    const { request: v1 } = IncrementalAlterConfigs.protocol({ version: 1 })(options);
    expect(v0.apiVersion).toBe(0);
    expect(v1.apiVersion).toBe(1);
  });
});
