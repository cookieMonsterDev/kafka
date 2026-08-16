import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeClusterResponseV1, responseSchema } from './response';

describe('protocol/requests/describe-cluster/v1/response', () => {
  it('round-trips a flexible v1 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 4,
      errorCode: 0,
      errorMessage: null,
      endpointType: 1,
      clusterId: 'MkU3OEVBNTcwNTJENDM2Qg',
      controllerId: 1,
      brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }],
      clusterAuthorizedOperations: 0,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeClusterResponseV1.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 4,
    });
    await expect(describeClusterResponseV1.parse(data)).resolves.toEqual(data);
  });

  it('throws on a mismatched endpoint type', async () => {
    await expect(
      describeClusterResponseV1.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 114,
        errorMessage: 'mismatched',
        endpointType: 2,
        clusterId: 'MkU3OEVBNTcwNTJENDM2Qg',
        controllerId: -1,
        brokers: [],
        clusterAuthorizedOperations: -2147483648,
      }),
    ).rejects.toMatchObject({ type: 'MISMATCHED_ENDPOINT_TYPE' });
  });
});
