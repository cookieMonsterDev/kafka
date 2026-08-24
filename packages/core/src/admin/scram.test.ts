import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { SCRAM_MECHANISMS } from '../protocol/enums/scram-mechanisms';
import { createScramApi } from './scram';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(
  broker: Record<string, ReturnType<typeof vi.fn>> = {
    describeUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }),
    alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }),
  },
) {
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

describe('admin/scram', () => {
  describe('describeUserScramCredentials', () => {
    it('lists credentials through the active controller', async () => {
      const broker = { describeUserScramCredentials: vi.fn().mockResolvedValue({ results: [{ user: 'alice' }] }) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(api.describeUserScramCredentials()).resolves.toEqual({ results: [{ user: 'alice' }] });
      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(broker.describeUserScramCredentials).toHaveBeenCalledWith({ users: undefined });
    });

    it('forwards the users filter to the controller broker', async () => {
      const broker = { describeUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await api.describeUserScramCredentials({ users: ['alice', 'bob'] });

      expect(broker.describeUserScramCredentials).toHaveBeenCalledWith({ users: ['alice', 'bob'] });
    });

    it('retries and rethrows on NOT_CONTROLLER, refreshing metadata again', async () => {
      const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const broker = { describeUserScramCredentials: vi.fn().mockRejectedValue(error) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
        retry: { retries: 0 },
      });

      await expect(api.describeUserScramCredentials()).rejects.toThrow();
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    });

    it('bails immediately on a non-retriable broker error', async () => {
      const error = new Error('boom');
      const broker = { describeUserScramCredentials: vi.fn().mockRejectedValue(error) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(api.describeUserScramCredentials()).rejects.toThrow('boom');
      expect(broker.describeUserScramCredentials).toHaveBeenCalledTimes(1);
    });
  });

  describe('alterUserScramCredentials', () => {
    it('rejects non-array deletions/upsertions', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      // @ts-expect-error intentionally invalid input
      await expect(api.alterUserScramCredentials({ deletions: 'nope' })).rejects.toThrow(KafkaNonRetriableError);
      expect(cluster.findControllerBroker).not.toHaveBeenCalled();
    });

    it('rejects when both deletions and upsertions are empty', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(api.alterUserScramCredentials({})).rejects.toThrow(
        'Must provide at least one SCRAM deletion or upsertion',
      );
    });

    it('rejects a deletion with an invalid mechanism', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(api.alterUserScramCredentials({ deletions: [{ name: 'alice', mechanism: 99 }] })).rejects.toThrow(
        'Invalid SCRAM mechanism 99',
      );
    });

    it('rejects a deletion missing a user name', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(
        api.alterUserScramCredentials({ deletions: [{ name: '', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }] }),
      ).rejects.toThrow('SCRAM deletion requires a user name');
    });

    it('rejects an upsertion with an invalid mechanism', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(
        api.alterUserScramCredentials({ upsertions: [{ name: 'alice', mechanism: 99, password: 'pencil' }] }),
      ).rejects.toThrow('Invalid SCRAM mechanism 99');
    });

    it('rejects an upsertion missing a user name', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(
        api.alterUserScramCredentials({
          upsertions: [{ name: '', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256, password: 'pencil' }],
        }),
      ).rejects.toThrow('SCRAM upsertion requires a user name');
    });

    it('rejects a password upsertion with too few iterations', async () => {
      const cluster = fakeCluster();
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(
        api.alterUserScramCredentials({
          upsertions: [
            {
              name: 'alice',
              mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256,
              password: 'pencil',
              iterations: 10,
            },
          ],
        }),
      ).rejects.toThrow('SCRAM iterations must be at least 4096');
    });

    it('derives the salted password via PBKDF2 for a SHA-256 password upsertion', async () => {
      const broker = { alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      const salt = Buffer.from('abcdefghijklmnop');
      await api.alterUserScramCredentials({
        upsertions: [{ name: 'alice', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256, password: 'pencil', salt }],
      });

      expect(broker.alterUserScramCredentials).toHaveBeenCalledWith({
        deletions: [],
        upsertions: [
          {
            name: 'alice',
            mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256,
            iterations: 4096,
            salt,
            saltedPassword: Buffer.from(
              '7dae684e3273dc0b27e8e7f24044febb7d9754149f6f9b34507b2e1a399b9371'.slice(0, 64),
              'hex',
            ),
          },
        ],
      });
    });

    it('derives the salted password via PBKDF2 for a SHA-512 password upsertion with custom iterations', async () => {
      const broker = { alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      const salt = Buffer.from('abcdefghijklmnop');
      await api.alterUserScramCredentials({
        upsertions: [
          {
            name: 'bob',
            mechanism: SCRAM_MECHANISMS.SCRAM_SHA_512,
            password: 'pencil',
            salt,
            iterations: 5000,
          },
        ],
      });

      expect(broker.alterUserScramCredentials).toHaveBeenCalledWith({
        deletions: [],
        upsertions: [
          {
            name: 'bob',
            mechanism: SCRAM_MECHANISMS.SCRAM_SHA_512,
            iterations: 5000,
            salt,
            saltedPassword: Buffer.from(
              'c379f0077d490e7c1aab00ecd454ac38ba07afada77e9f522f465367497bbac5c4f8f2c9c794daf271b9a83fcacd93bad47bae7a35415d218bc314febe6b14b2',
              'hex',
            ),
          },
        ],
      });
    });

    it('passes a pre-salted upsertion straight through without hashing', async () => {
      const broker = { alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      const salt = Buffer.from('salt');
      const saltedPassword = Buffer.from('already-hashed');
      await api.alterUserScramCredentials({
        upsertions: [
          { name: 'carol', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256, iterations: 8192, salt, saltedPassword },
        ],
      });

      expect(broker.alterUserScramCredentials).toHaveBeenCalledWith({
        deletions: [],
        upsertions: [
          { name: 'carol', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256, iterations: 8192, salt, saltedPassword },
        ],
      });
    });

    it('sends deletions alongside upsertions and returns the broker results', async () => {
      const broker = {
        alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [{ user: 'alice', errorCode: 0 }] }),
      };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      const result = await api.alterUserScramCredentials({
        deletions: [{ name: 'alice', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }],
      });

      expect(result).toEqual({ results: [{ user: 'alice', errorCode: 0 }] });
      expect(broker.alterUserScramCredentials).toHaveBeenCalledWith({
        deletions: [{ name: 'alice', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }],
        upsertions: [],
      });
    });

    it('retries and rethrows on NOT_CONTROLLER, refreshing metadata again', async () => {
      const error = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const broker = { alterUserScramCredentials: vi.fn().mockRejectedValue(error) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
        retry: { retries: 0 },
      });

      await expect(
        api.alterUserScramCredentials({ deletions: [{ name: 'alice', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }] }),
      ).rejects.toThrow();
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    });

    it('bails immediately on a non-retriable broker error', async () => {
      const error = new Error('boom');
      const broker = { alterUserScramCredentials: vi.fn().mockRejectedValue(error) };
      const cluster = fakeCluster(broker);
      const api = createScramApi({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger.namespace('Admin'),
        rootLogger: silentLogger,
      });

      await expect(
        api.alterUserScramCredentials({ deletions: [{ name: 'alice', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }] }),
      ).rejects.toThrow('boom');
      expect(broker.alterUserScramCredentials).toHaveBeenCalledTimes(1);
    });
  });
});
