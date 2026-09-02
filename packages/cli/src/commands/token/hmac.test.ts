import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeStdin } from '../../testing/create-fake-stdin';
import { resolveHmacFlag } from './hmac';

describe('resolveHmacFlag', () => {
  it('decodes --hmac as base64', async () => {
    const stdin = createFakeStdin();
    const hmac = await resolveHmacFlag({ hmacFlag: Buffer.from('secret').toString('base64'), hmacStdin: false, stdin });
    expect(hmac).toEqual(Buffer.from('secret'));
  });

  it('reads and decodes --hmac-stdin', async () => {
    const stdin = createFakeStdin();
    const promise = resolveHmacFlag({ hmacFlag: undefined, hmacStdin: true, stdin });
    stdin.emitData(Buffer.from('secret').toString('base64'));
    stdin.emitEnd();
    await expect(promise).resolves.toEqual(Buffer.from('secret'));
  });

  it('rejects passing both --hmac and --hmac-stdin', async () => {
    const stdin = createFakeStdin();
    await expect(
      resolveHmacFlag({ hmacFlag: Buffer.from('secret').toString('base64'), hmacStdin: true, stdin }),
    ).rejects.toThrow(/mutually exclusive/);
  });

  it('rejects when neither --hmac nor --hmac-stdin is given', async () => {
    const stdin = createFakeStdin();
    await expect(resolveHmacFlag({ hmacFlag: undefined, hmacStdin: false, stdin })).rejects.toThrow(CliUsageError);
  });

  it('rejects an empty --hmac-stdin read', async () => {
    const stdin = createFakeStdin();
    const promise = resolveHmacFlag({ hmacFlag: undefined, hmacStdin: true, stdin });
    stdin.emitEnd();
    await expect(promise).rejects.toThrow(CliUsageError);
  });
});
