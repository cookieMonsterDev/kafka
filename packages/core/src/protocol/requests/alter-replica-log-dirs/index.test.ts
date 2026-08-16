import { describe, expect, it } from 'vitest';
import { AlterReplicaLogDirs } from './index';

describe('protocol/requests/alter-replica-log-dirs', () => {
  it('implements versions 0 through 2', () => {
    expect(AlterReplicaLogDirs.versions).toEqual([0, 1, 2]);
  });

  it('builds a request for the requested version', () => {
    const options = { dirs: [{ path: '/var/kafka/data-1', topics: [{ topic: 'orders', partitions: [0] }] }] };
    expect(AlterReplicaLogDirs.protocol({ version: 0 })(options).request.apiVersion).toBe(0);
    expect(AlterReplicaLogDirs.protocol({ version: 1 })(options).request.apiVersion).toBe(1);
    expect(AlterReplicaLogDirs.protocol({ version: 2 })(options).request.apiVersion).toBe(2);
  });
});
