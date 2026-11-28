import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { deleteGroupsResponseV3 } from './response';

function encodeResult(encoder: Encoder, groupId: string, errorCode: number, errorMessage: string | null): Encoder {
  return encoder.writeUVarIntString(groupId).writeInt16(errorCode).writeUVarIntString(errorMessage).writeUVarInt(0);
}

describe('protocol/requests/delete-groups/v3/response', () => {
  it('decodes a successful result without an error and remaps throttleTime', async () => {
    const rawData = encodeResult(new Encoder().writeInt32(7).writeUVarInt(2), 'g1', 0, null).writeUVarInt(0).buffer;
    const data = await deleteGroupsResponseV3.decode(rawData);

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 7, results: [{ groupId: 'g1', errorCode: 0 }] });
    await expect(deleteGroupsResponseV3.parse(data)).resolves.toBe(data);
  });

  it("attaches each failed result's error with the broker message, or the generic one when absent", async () => {
    let encoder = new Encoder().writeInt32(0).writeUVarInt(3);
    encoder = encodeResult(encoder, 'g1', 134, 'Streams topology plugin failed');
    encoder = encodeResult(encoder, 'g2', 69, null);
    const data = await deleteGroupsResponseV3.decode(encoder.writeUVarInt(0).buffer);

    const [deletionFailed, notFound] = data.results;
    expect(deletionFailed?.error).toBeInstanceOf(KafkaProtocolError);
    expect(deletionFailed).toMatchObject({
      groupId: 'g1',
      errorCode: 134,
      error: { type: 'GROUP_DELETION_FAILED', retriable: false, message: 'Streams topology plugin failed' },
    });
    expect(notFound).toMatchObject({
      groupId: 'g2',
      errorCode: 69,
      error: { type: 'GROUP_ID_NOT_FOUND', message: 'The group id was not found' },
    });
    await expect(deleteGroupsResponseV3.parse(data)).resolves.toBe(data);
  });
});
