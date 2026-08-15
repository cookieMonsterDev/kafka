import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { createPartitionsRequestV0, withAssignmentDefaults } from './request.js';

describe('protocol/requests/create-partitions/v0/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await createPartitionsRequestV0({
      topicPartitions: withAssignmentDefaults([
        { topic: 'test-topic-c8d8ca3d95495c6b900d', count: 3 },
        { topic: 'test-topic-050fb2e6aed13a954288', count: 5, assignments: [[0], [1], [2]] },
      ]),
      timeout: 5000,
      validateOnly: false,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
