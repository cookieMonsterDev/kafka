import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeClusterResponseV0, responseSchema } from './response';

describe('protocol/requests/describe-cluster/v0/response', () => {
  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 7,
      errorCode: 0,
      errorMessage: null,
      clusterId: 'MkU3OEVBNTcwNTJENDM2Qg',
      controllerId: 1,
      brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: 'az-1' }],
      clusterAuthorizedOperations: -2147483648,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeClusterResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 7,
    });
    await expect(describeClusterResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error code', async () => {
    await expect(
      describeClusterResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 31,
        errorMessage: 'unauthorized',
        clusterId: 'MkU3OEVBNTcwNTJENDM2Qg',
        controllerId: -1,
        brokers: [],
        clusterAuthorizedOperations: -2147483648,
      }),
    ).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });
});
