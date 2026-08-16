import { describe, expect, it } from 'vitest';
import { Heartbeat } from './index';

describe('protocol/requests/heartbeat', () => {
  it('implements versions 0 through 4', () => {
    expect(Heartbeat.versions).toEqual([0, 1, 2, 3, 4]);
  });

  it('defaults groupInstanceId to null on v3 when omitted', async () => {
    const { request } = Heartbeat.protocol({ version: 3 })({
      groupId: 'group',
      groupGenerationId: 1,
      memberId: 'member',
    });
    const encoder = await request.encode();
    expect(encoder.buffer.subarray(-2)).toEqual(Buffer.from([0xff, 0xff]));
  });

  it('defaults groupInstanceId to compact null on v4 when omitted', async () => {
    const { request } = Heartbeat.protocol({ version: 4 })({
      groupId: 'group',
      groupGenerationId: 1,
      memberId: 'member',
    });
    const encoder = await request.encode();
    expect(request.apiVersion).toBe(4);
    expect(encoder.buffer.subarray(-2)).toEqual(Buffer.from([0, 0]));
  });
});
