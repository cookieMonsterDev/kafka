import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { ConsumerGroupHeartbeat } from './index';
import { requestSchema as requestSchemaV0 } from './v0/request';
import { requestSchema as requestSchemaV1 } from './v1/request';

describe('protocol/requests/consumer-group-heartbeat', () => {
  it('implements versions 0 and 1', () => {
    expect(ConsumerGroupHeartbeat.versions).toEqual([0, 1]);
  });

  it('creates a version 0 request without the regex field', async () => {
    const { request } = ConsumerGroupHeartbeat.protocol({ version: 0 })({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      subscribedTopicNames: ['events'],
      subscribedTopicRegex: 'ignored',
    });
    expect(request).toMatchObject({ apiKey: 68, apiVersion: 0, apiName: 'ConsumerGroupHeartbeat' });

    const encoder = await request.encode();
    expect(requestSchemaV0.read(new Decoder(encoder.buffer))).toEqual({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      instanceId: null,
      rackId: null,
      rebalanceTimeoutMs: -1,
      subscribedTopicNames: ['events'],
      serverAssignor: null,
      topicPartitions: null,
    });
  });

  it('creates a version 1 request with subscribedTopicRegex', async () => {
    const { request } = ConsumerGroupHeartbeat.protocol({ version: 1 })({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      subscribedTopicRegex: 'events-.*',
    });
    expect(request).toMatchObject({ apiKey: 68, apiVersion: 1, apiName: 'ConsumerGroupHeartbeat' });

    const encoder = await request.encode();
    expect(requestSchemaV1.read(new Decoder(encoder.buffer))).toEqual({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      instanceId: null,
      rackId: null,
      rebalanceTimeoutMs: -1,
      subscribedTopicNames: null,
      subscribedTopicRegex: 'events-.*',
      serverAssignor: null,
      topicPartitions: null,
    });
  });
});
