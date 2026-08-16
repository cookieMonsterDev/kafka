import { describe, expect, it, vi } from 'vitest';
import type { Broker } from '../broker/index';
import type { Cluster } from '../cluster/index';
import { KafkaMetadataNotLoaded } from '../errors';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import {
  findTopicPartitions,
  formatUnknown,
  groupResourcesByBroker,
  isBrokerConfig,
  isConsumerGroupIdle,
  protocolType,
  requireMetadata,
  retryOnLeaderNotAvailable,
} from './helpers';

describe('admin/helpers', () => {
  describe('formatUnknown', () => {
    it('stringifies arbitrary values', () => {
      expect(formatUnknown('abc')).toBe('abc');
      expect(formatUnknown(12)).toBe('12');
      expect(formatUnknown(null)).toBe('null');
    });
  });

  describe('protocolType', () => {
    it('reads a string type off an error-like object', () => {
      expect(protocolType({ type: 'LEADER_NOT_AVAILABLE' })).toBe('LEADER_NOT_AVAILABLE');
    });

    it('returns undefined for anything without a string type', () => {
      expect(protocolType(undefined)).toBeUndefined();
      expect(protocolType('LEADER_NOT_AVAILABLE')).toBeUndefined();
      expect(protocolType({ message: 'no type' })).toBeUndefined();
      expect(protocolType({ type: 5 })).toBeUndefined();
    });
  });

  describe('retryOnLeaderNotAvailable', () => {
    it('retries while the error is a stale-metadata type, then returns', async () => {
      let attempts = 0;
      const result = await retryOnLeaderNotAvailable(
        async () => {
          attempts += 1;
          if (attempts < 3) {
            throw Object.assign(new Error('no leader'), { type: 'LEADER_NOT_AVAILABLE' });
          }
          return 'ready';
        },
        { delay: 1, maxWait: 200 },
      );

      expect(result).toBe('ready');
      expect(attempts).toBe(3);
    });

    it('rethrows errors that are not stale metadata', async () => {
      await expect(
        retryOnLeaderNotAvailable(
          async () => {
            throw new Error('boom');
          },
          { delay: 1, maxWait: 50 },
        ),
      ).rejects.toThrow('boom');
    });
  });

  describe('findTopicPartitions', () => {
    it('loads metadata and returns sorted partition ids', async () => {
      const cluster = {
        addTargetTopic: vi.fn().mockResolvedValue(undefined),
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicPartitionMetadata: vi
          .fn()
          .mockReturnValue([{ partitionId: 2 }, { partitionId: 0 }, { partitionId: 1 }]),
      };

      await expect(findTopicPartitions(cluster as unknown as Cluster, 'orders')).resolves.toEqual([0, 1, 2]);
      expect(cluster.addTargetTopic).toHaveBeenCalledWith('orders');
      expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled();
    });
  });

  describe('requireMetadata', () => {
    it('returns cluster metadata when present', async () => {
      const metadata = { topicMetadata: [], brokers: [], clusterId: 'c', controllerId: 1 };
      const cluster = { metadata: vi.fn().mockResolvedValue(metadata) };
      await expect(requireMetadata(cluster as unknown as Cluster)).resolves.toBe(metadata);
      expect(cluster.metadata).toHaveBeenCalledWith({});
    });

    it('throws KafkaMetadataNotLoaded when metadata is missing', async () => {
      const cluster = { metadata: vi.fn().mockResolvedValue(undefined) };
      await expect(requireMetadata(cluster as unknown as Cluster, { topics: ['t'] })).rejects.toThrow(
        KafkaMetadataNotLoaded,
      );
      expect(cluster.metadata).toHaveBeenCalledWith({ topics: ['t'] });
    });
  });

  describe('isConsumerGroupIdle', () => {
    it('is true for Empty and Dead, false otherwise', () => {
      expect(isConsumerGroupIdle('Empty')).toBe(true);
      expect(isConsumerGroupIdle('Dead')).toBe(true);
      expect(isConsumerGroupIdle('Stable')).toBe(false);
      expect(isConsumerGroupIdle('PreparingRebalance')).toBe(false);
    });
  });

  describe('isBrokerConfig', () => {
    it('is true for broker and broker-logger resources', () => {
      expect(isBrokerConfig(CONFIG_RESOURCE_TYPES.BROKER)).toBe(true);
      expect(isBrokerConfig(CONFIG_RESOURCE_TYPES.BROKER_LOGGER)).toBe(true);
      expect(isBrokerConfig(CONFIG_RESOURCE_TYPES.TOPIC)).toBe(false);
      expect(isBrokerConfig(CONFIG_RESOURCE_TYPES.UNKNOWN)).toBe(false);
    });
  });

  describe('groupResourcesByBroker', () => {
    it('routes broker resources to the named broker and everything else to the default', async () => {
      const defaultBroker = { nodeId: 1 };
      const named = { nodeId: 2 };
      const cluster = { findBroker: vi.fn().mockResolvedValue(named) };

      const grouped = await groupResourcesByBroker({
        cluster: cluster as unknown as Cluster,
        defaultBroker: defaultBroker as unknown as Broker,
        resources: [
          { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' },
          { type: CONFIG_RESOURCE_TYPES.BROKER, name: '2' },
          { type: CONFIG_RESOURCE_TYPES.BROKER_LOGGER, name: '2' },
        ],
      });

      expect(grouped.get(defaultBroker as unknown as Broker)).toEqual([
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' },
      ]);
      expect(grouped.get(named as unknown as Broker)).toEqual([
        { type: CONFIG_RESOURCE_TYPES.BROKER, name: '2' },
        { type: CONFIG_RESOURCE_TYPES.BROKER_LOGGER, name: '2' },
      ]);
      expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '2' });
    });
  });
});
