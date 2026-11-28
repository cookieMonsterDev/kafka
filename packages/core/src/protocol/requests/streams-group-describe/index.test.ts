import { describe, expect, it } from 'vitest';
import { StreamsGroupDescribe } from './index';

describe('protocol/requests/streams-group-describe', () => {
  it('implements versions 0 and 1', () => {
    expect(StreamsGroupDescribe.versions).toEqual([0, 1]);
  });

  it('builds version 0 and 1 requests', () => {
    const options = { groupIds: ['app'], includeTopologyDescription: false };
    expect(StreamsGroupDescribe.protocol({ version: 0 })(options).request).toMatchObject({
      apiKey: 89,
      apiVersion: 0,
      apiName: 'StreamsGroupDescribe',
    });
    expect(StreamsGroupDescribe.protocol({ version: 1 })(options).request).toMatchObject({
      apiKey: 89,
      apiVersion: 1,
      apiName: 'StreamsGroupDescribe',
    });
  });

  it('throws on version 0 when asked for topology descriptions', () => {
    expect(() =>
      StreamsGroupDescribe.protocol({ version: 0 })({ groupIds: ['app'], includeTopologyDescription: true }),
    ).toThrow(/needs StreamsGroupDescribe v1/);
  });
});
