import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeClusterResponseV2, responseSchema } from './response';

describe('protocol/requests/describe-cluster/v2/response', () => {
  it('round-trips a flexible v2 response and remaps throttleTime', async () => {
    const value = {
      throttleTime: 9,
      errorCode: 0,
      errorMessage: null,
      endpointType: 1,
      clusterId: 'MkU3OEVBNTcwNTJENDM2Qg',
      controllerId: 1,
      brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: 'az-1', isFenced: false }],
      clusterAuthorizedOperations: 0,
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeClusterResponseV2.decode(encoder.buffer);
    expect(data).toEqual({
      ...value,
      throttleTime: 0,
      clientSideThrottleTime: 9,
    });
    await expect(describeClusterResponseV2.parse(data)).resolves.toEqual(data);
  });
});
