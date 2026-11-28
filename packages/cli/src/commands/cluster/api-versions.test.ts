import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterApiVersionsCommand } from './api-versions';

function fakeApiVersions() {
  return {
    brokers: [
      {
        nodeId: 1,
        host: 'kafka-1',
        port: 9092,
        rack: null,
        apis: [
          {
            apiKey: 0,
            apiName: 'Produce',
            brokerVersions: { minVersion: 3, maxVersion: 13 },
            isImplemented: true,
            version: 13,
          },
          { apiKey: 1, apiName: 'Fetch', isImplemented: true },
          {
            apiKey: 2,
            apiName: 'ListOffsets',
            brokerVersions: { minVersion: 50, maxVersion: 60 },
            isImplemented: true,
          },
          {
            apiKey: 63,
            apiName: 'BrokerHeartbeat',
            brokerVersions: { minVersion: 0, maxVersion: 1 },
            isImplemented: false,
          },
          { apiKey: 999, brokerVersions: { minVersion: 2, maxVersion: 2 }, isImplemented: false },
        ],
      },
      {
        nodeId: 2,
        host: 'kafka-2',
        port: 9093,
        rack: 'rack-b',
        apis: [
          {
            apiKey: 18,
            apiName: 'ApiVersions',
            brokerVersions: { minVersion: 0, maxVersion: 5 },
            isImplemented: true,
            version: 5,
          },
        ],
      },
    ],
  };
}

describe('clusterApiVersionsCommand', () => {
  it('prints one kafka-cluster.sh api-versions block per broker', async () => {
    const describeApiVersions = vi.fn(async () => fakeApiVersions());
    const admin = createFakeAdmin({ describeApiVersions, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await clusterApiVersionsCommand.run(context);

    expect(code).toBe(0);
    expect(describeApiVersions).toHaveBeenCalledWith({});
    expect(stdoutWrite.mock.calls[0]![0]).toBe(
      [
        'kafka-1:9092 (id: 1 rack: null) -> (',
        '\tProduce(0): 3 to 13 [usable: 13],',
        '\tFetch(1): UNSUPPORTED,',
        '\tListOffsets(2): 50 to 60 [unusable: no common version],',
        '\tBrokerHeartbeat(63): 0 to 1 [not implemented],',
        '\tUNKNOWN(999): 2 [not implemented]',
        ')',
        'kafka-2:9093 (id: 2 rack: rack-b) -> (',
        '\tApiVersions(18): 0 to 5 [usable: 5]',
        ')\n',
      ].join('\n'),
    );
  });

  it('describes only the broker named by --node', async () => {
    const describeApiVersions = vi.fn(async () => fakeApiVersions());
    const admin = createFakeAdmin({ describeApiVersions, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', node: 2 },
      openAdmin: async () => admin,
    });

    await clusterApiVersionsCommand.run(context);

    expect(describeApiVersions).toHaveBeenCalledWith({ brokerIds: [2] });
  });

  it('prints the core result with --json', async () => {
    const admin = createFakeAdmin({ describeApiVersions: async () => fakeApiVersions(), disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      format: 'json',
      openAdmin: async () => admin,
    });

    await clusterApiVersionsCommand.run(context);

    expect(JSON.parse(stdoutWrite.mock.calls[0]![0])).toEqual(fakeApiVersions());
  });

  it('disconnects when the call fails', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeApiVersions: async () => {
        throw new Error('Broker 7 is not in the cluster metadata');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', node: 7 },
      openAdmin: async () => admin,
    });

    await expect(clusterApiVersionsCommand.run(context)).rejects.toThrow('Broker 7 is not in the cluster metadata');
    expect(disconnect).toHaveBeenCalledOnce();
  });
});
