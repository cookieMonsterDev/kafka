import { describe, expect, it } from 'vitest';
import { AlterUserScramCredentials } from './index';

describe('protocol/requests/alter-user-scram-credentials', () => {
  it('implements version 0', () => {
    expect(AlterUserScramCredentials.versions).toEqual([0]);
  });

  it('defaults omissions to empty arrays', async () => {
    const { request } = AlterUserScramCredentials.protocol({ version: 0 })({});
    expect(request.apiVersion).toBe(0);
    const encoder = await request.encode();
    expect(encoder.buffer).toEqual(Buffer.from([1, 1, 0]));
  });
});
