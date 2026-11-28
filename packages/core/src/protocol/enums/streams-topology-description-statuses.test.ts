import { describe, expect, it } from 'vitest';
import { STREAMS_TOPOLOGY_DESCRIPTION_STATUSES } from './streams-topology-description-statuses';

describe('protocol/enums/streams-topology-description-statuses', () => {
  it('matches the StreamsGroupDescribe topology_description_status wire values', () => {
    expect(STREAMS_TOPOLOGY_DESCRIPTION_STATUSES).toEqual({ NOT_REQUESTED: 0, NOT_STORED: 1, ERROR: 2, AVAILABLE: 3 });
    expect(Object.isFrozen(STREAMS_TOPOLOGY_DESCRIPTION_STATUSES)).toBe(true);
  });
});
