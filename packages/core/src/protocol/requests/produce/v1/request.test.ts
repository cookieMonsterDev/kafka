import { describe, expect, it } from 'vitest';
import { produceRequestV0 } from '../v0/request';
import { produceRequestV1 } from './request';

describe('protocol/requests/produce/v1/request', () => {
  const options = { acks: -1, timeout: 1000, topicData: [] };

  it('shares the v0 wire shape and sets apiVersion 1', async () => {
    const v0 = produceRequestV0(options);
    const v1 = produceRequestV1(options);
    expect(v1.apiKey).toBe(v0.apiKey);
    expect(v1.apiName).toBe(v0.apiName);
    expect(v1.apiVersion).toBe(1);
    expect((await v1.encode()).buffer).toEqual((await v0.encode()).buffer);
  });
});
