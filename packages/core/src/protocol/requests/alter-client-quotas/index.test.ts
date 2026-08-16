import { describe, expect, it } from 'vitest';
import { AlterClientQuotas } from './index';

describe('protocol/requests/alter-client-quotas', () => {
  it('implements versions 0 through 1', () => {
    expect(AlterClientQuotas.versions).toEqual([0, 1]);
  });

  it('builds a request for the requested version', () => {
    const options = {
      entries: [
        {
          entity: [{ entityType: 'client-id', entityName: 'orders-producer' }],
          ops: [{ key: 'producer_byte_rate', value: 1048576, remove: false }],
        },
      ],
    };
    const { request: v0 } = AlterClientQuotas.protocol({ version: 0 })(options);
    const { request: v1 } = AlterClientQuotas.protocol({ version: 1 })(options);
    expect(v0.apiVersion).toBe(0);
    expect(v1.apiVersion).toBe(1);
  });
});
