import { afterEach, describe, expect, it } from 'vitest';
import { createBrokerPool } from '../../helpers/index.js';

describe('cluster.brokerPool', () => {
  let brokerPool: ReturnType<typeof createBrokerPool> | undefined;

  afterEach(async () => {
    await brokerPool?.disconnect();
  });

  it('defaults metadataMaxAge to 0', () => {
    brokerPool = createBrokerPool();
    expect(brokerPool.metadataMaxAge).toBe(0);
  });

  it('connects the seed broker and loads versions', async () => {
    brokerPool = createBrokerPool();
    expect(brokerPool.seedBroker).toBeUndefined();
    await brokerPool.connect();
    expect(brokerPool.seedBroker?.isConnected()).toBe(true);
    expect(brokerPool.versions).toEqual(brokerPool.seedBroker?.versions);
  });

  it('is a no-op when already connected', async () => {
    brokerPool = createBrokerPool();
    await brokerPool.connect();
    await brokerPool.connect();
    expect(brokerPool.seedBroker?.isConnected()).toBe(true);
  });
});
