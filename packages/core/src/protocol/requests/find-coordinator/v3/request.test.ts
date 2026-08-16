import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { findCoordinatorRequestV2 } from '../v2/request';
import { findCoordinatorRequestV3, requestSchema } from './request';

const payload = { coordinatorKey: 'group-id', coordinatorType: 0 };

describe('protocol/requests/find-coordinator/v3/request', () => {
  it('encodes compact coordinatorKey and a trailing TAG_BUFFER', async () => {
    const definition = findCoordinatorRequestV3(payload);
    expect(definition.apiVersion).toBe(3);
    expect(definition.apiName).toBe('GroupCoordinator');

    const encoder = await definition.encode();
    const expected = new Encoder().writeUVarIntString('group-id').writeInt8(0).writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v2 encoding', async () => {
    const v3 = await findCoordinatorRequestV3(payload).encode();
    const v2 = await findCoordinatorRequestV2(payload).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
