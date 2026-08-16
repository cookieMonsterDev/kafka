import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { DescribeCluster } from './index';
import { requestSchema as requestSchemaV1 } from './v1/request';
import { requestSchema as requestSchemaV2 } from './v2/request';

describe('protocol/requests/describe-cluster', () => {
  it('implements versions 0 through 2', () => {
    expect(DescribeCluster.versions).toEqual([0, 1, 2]);
  });

  it('builds a request for the requested version', () => {
    const options = { includeClusterAuthorizedOperations: true };
    expect(DescribeCluster.protocol({ version: 0 })(options).request.apiVersion).toBe(0);
    expect(DescribeCluster.protocol({ version: 1 })(options).request.apiVersion).toBe(1);
    expect(DescribeCluster.protocol({ version: 2 })(options).request.apiVersion).toBe(2);
  });

  it('defaults v1 endpointType to brokers and v2 includeFencedBrokers to false', async () => {
    const { request: v1 } = DescribeCluster.protocol({ version: 1 })({});
    expect(requestSchemaV1.read(new Decoder((await v1.encode()).buffer))).toEqual({
      includeClusterAuthorizedOperations: false,
      endpointType: 1,
    });

    const { request: v2 } = DescribeCluster.protocol({ version: 2 })({});
    expect(requestSchemaV2.read(new Decoder((await v2.encode()).buffer))).toEqual({
      includeClusterAuthorizedOperations: false,
      endpointType: 1,
      includeFencedBrokers: false,
    });
  });
});
