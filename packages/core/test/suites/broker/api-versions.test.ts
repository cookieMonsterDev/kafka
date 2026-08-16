import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index.js';
import { API_KEYS } from '../../../src/protocol/requests/api-keys.js';
import { connectionOpts, createConnectionPool, newLogger } from '../../helpers/index.js';

describe('broker.apiVersions', () => {
  let broker: Broker | undefined;

  beforeEach(async () => {
    broker = new Broker({ connectionPool: createConnectionPool(connectionOpts()), logger: newLogger() });
    await broker.connect();
  });

  afterEach(async () => {
    await broker?.disconnect();
  });

  it('returns broker API versions', async () => {
    const versions = await broker!.apiVersions();
    expect(versions[API_KEYS.Metadata]).toEqual(expect.objectContaining({ minVersion: expect.any(Number) }));
    expect(versions[API_KEYS.Produce]).toEqual(expect.objectContaining({ maxVersion: expect.any(Number) }));
  });

  it('negotiates a supported ApiVersions version on connect', async () => {
    expect(broker!.isConnected()).toBe(true);
    const versions = await broker!.apiVersions();
    expect(versions[API_KEYS.ApiVersions]?.maxVersion).toBeGreaterThanOrEqual(0);
  });
});
