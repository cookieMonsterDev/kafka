import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { findCoordinatorResponseV0 } from './response';

/** The broker always sends the full coordinator shape, even on error (dummy values). */
function unsupportedVersionResponse(): Buffer {
  return new Encoder().writeInt16(35).writeInt32(-1).writeString('').writeInt32(-1).buffer;
}

describe('protocol/requests/find-coordinator/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await findCoordinatorResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({ errorCode: 0, coordinator: { nodeId: 1, host: '192.168.1.155', port: 9095 } });
    await expect(findCoordinatorResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws a KafkaProtocolError if the api is not supported', async () => {
    const data = await findCoordinatorResponseV0.decode(unsupportedVersionResponse());
    await expect(findCoordinatorResponseV0.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
