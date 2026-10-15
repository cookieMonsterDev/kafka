import { describe, expect, it } from 'vitest';
import { ElectLeaders } from './index';

describe('protocol/requests/elect-leaders', () => {
  it('implements versions 0 through 2', () => {
    expect(ElectLeaders.versions).toEqual([0, 1, 2]);
  });

  it('builds a request for the requested version', () => {
    const options = { topicPartitions: [{ topic: 'orders', partitions: [0] }] };
    expect(ElectLeaders.protocol({ version: 0 })(options).request.apiVersion).toBe(0);
    expect(ElectLeaders.protocol({ version: 1 })(options).request.apiVersion).toBe(1);
    expect(ElectLeaders.protocol({ version: 2 })(options).request.apiVersion).toBe(2);
  });

  it.each([0, 1, 2])('v%i encodes an empty topicPartitions list as empty, not null (all)', async (version) => {
    const encode = (options: Parameters<ReturnType<typeof ElectLeaders.protocol>>[0]) =>
      ElectLeaders.protocol({ version })(options).request.encode();
    const empty = (await encode({ topicPartitions: [] })).buffer;
    const all = (await encode({})).buffer;
    expect((await encode({ topicPartitions: null })).buffer).toEqual(all);
    expect(empty).not.toEqual(all);
  });
});
