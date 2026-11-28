import { describe, expect, it } from 'vitest';
import { KAFKA_APIS } from './kafka-apis';
import { loadImplementedApis } from './request-families';

describe('protocol/requests/request-families', () => {
  it('has one family for each Kafka API this client implements', async () => {
    const apis = await loadImplementedApis();

    expect(apis.map(({ apiKey }) => apiKey).sort((a, b) => a - b)).toEqual(
      Object.values(KAFKA_APIS)
        .map(({ apiKey }) => apiKey)
        .sort((a, b) => a - b),
    );
  });
});
