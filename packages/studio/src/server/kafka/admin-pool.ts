import type { Admin } from '@cookiemonsterdev/kafka-core';
import { ProfileClientPool } from './profile-client-pool';

export type PooledAdmin = Pick<
  Admin,
  | 'connect'
  | 'disconnect'
  | 'describeCluster'
  | 'listTopics'
  | 'fetchTopicMetadata'
  | 'describeTopicPartitions'
  | 'fetchTopicOffsets'
  | 'fetchTopicOffsetsByTimestamp'
  | 'describeConfigs'
  | 'describeLogDirs'
  | 'createTopics'
  | 'deleteTopics'
  | 'deleteTopicRecords'
  | 'createPartitions'
  | 'incrementalAlterConfigs'
  | 'listGroups'
  | 'describeGroups'
  | 'fetchOffsets'
  | 'setOffsets'
  | 'resetOffsets'
  | 'deleteGroups'
  | 'deleteGroupOffsets'
  | 'removeMembersFromConsumerGroup'
  | 'describeShareGroups'
  | 'listShareGroupOffsets'
  | 'describeAcls'
  | 'describeClientQuotas'
  | 'listTransactions'
>;

/** Everything `AdminPool` needs to build a fresh, unconnected client for one profile. */
export type KafkaClientFactory = (profileName: string | null) => { admin(): PooledAdmin };

/** A profile-scoped pool retaining the admin's typed capabilities. */
export class AdminPool extends ProfileClientPool<PooledAdmin> {
  constructor(createClient: KafkaClientFactory) {
    super((profile) => createClient(profile).admin());
  }
}
