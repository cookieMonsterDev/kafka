import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { apiVersionsResponseV5 } from './response';

function v5ResponseBuffer(errorCode: number, apiKeys: { apiKey: number; minVersion: number; maxVersion: number }[]) {
  const entries = apiKeys.map((entry) =>
    new Encoder().writeInt16(entry.apiKey).writeInt16(entry.minVersion).writeInt16(entry.maxVersion).writeUVarInt(0),
  );

  return new Encoder().writeInt16(errorCode).writeUVarIntArray(entries).writeInt32(0).writeUVarInt(0).buffer;
}

describe('protocol/requests/api-versions/v5/response', () => {
  it('decodes the same body as v3 and v4', async () => {
    const data = await apiVersionsResponseV5.decode(
      v5ResponseBuffer(0, [{ apiKey: 18, minVersion: 0, maxVersion: 5 }]),
    );

    expect(data.apiVersions).toEqual([{ apiKey: 18, minVersion: 0, maxVersion: 5 }]);
    await expect(apiVersionsResponseV5.parse(data)).resolves.toBeTruthy();
  });

  it('throws REBOOTSTRAP_REQUIRED when the broker is not the expected node or cluster (KIP-1242)', async () => {
    const data = await apiVersionsResponseV5.decode(v5ResponseBuffer(129, []));
    await expect(apiVersionsResponseV5.parse(data)).rejects.toMatchObject({ type: 'REBOOTSTRAP_REQUIRED', code: 129 });
  });
});
