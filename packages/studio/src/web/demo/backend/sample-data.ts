/** The demo cluster's starting state and sample records. Seeded, so every visit opens on the same data. */

import type { ApiVersionView } from '../../../shared/contracts/cluster';
import type { GroupMember, StreamsGroupDetailResponse } from '../../../shared/contracts/group';
import type { ProduceMessage } from '../../../shared/contracts/produce';
import type { AclEntryView, QuotaEntryView, TransactionListEntry } from '../../../shared/contracts/settings';

export type RandomSource = () => number;

/** mulberry32: a tiny seeded PRNG. Quality is irrelevant here; reproducibility is the point. */
export function createRandom(seed: number): RandomSource {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
}

function pick<T>(random: RandomSource, values: readonly T[]): T {
  const value = values[Math.floor(random() * values.length)];
  if (value === undefined) throw new Error('pick() needs at least one value');
  return value;
}

function hex(random: RandomSource, length: number): string {
  let out = '';
  for (let index = 0; index < length; index++) out += Math.floor(random() * 16).toString(16);
  return out;
}

function money(random: RandomSource, min: number, max: number): number {
  return Math.round((min + random() * (max - min)) * 100) / 100;
}

function traceHeaders(random: RandomSource, source: string): Record<string, string> {
  return { source, 'trace-id': hex(random, 16), 'content-type': 'application/json' };
}

const CUSTOMERS = ['cus_1042', 'cus_2077', 'cus_3150', 'cus_4211', 'cus_5309', 'cus_6120', 'cus_7004', 'cus_8833'];
const SKUS = ['SKU-KB-001', 'SKU-MS-014', 'SKU-HD-220', 'SKU-CB-031', 'SKU-MN-270', 'SKU-DK-118'];

/** The only connection profile the demo offers; the real Studio lists every configured one. */
export const DEMO_PROFILE = 'local';

type SampleGenerator = (random: RandomSource, sequence: number) => ProduceMessage;

export interface TopicSeed {
  readonly name: string;
  readonly partitions: number;
  readonly replicationFactor: number;
  readonly records: number;
  readonly overrides?: Readonly<Record<string, string>>;
  /** Chance per traffic tick that another application writes one record. */
  readonly trafficChance: number;
  readonly generate: SampleGenerator;
}

export const TOPIC_SEEDS: readonly TopicSeed[] = [
  {
    name: 'orders',
    partitions: 6,
    replicationFactor: 3,
    records: 1_200,
    overrides: { 'retention.ms': '259200000', 'min.insync.replicas': '2', 'compression.type': 'zstd' },
    trafficChance: 0.8,
    generate: (random, sequence) => {
      const orderId = `ord_${String(10_000 + sequence)}`;
      return {
        key: orderId,
        value: JSON.stringify({
          orderId,
          customerId: pick(random, CUSTOMERS),
          status: pick(random, ['created', 'created', 'paid', 'packed', 'shipped']),
          items: [{ sku: pick(random, SKUS), quantity: 1 + Math.floor(random() * 3) }],
          total: money(random, 12, 480),
          currency: 'EUR',
        }),
        headers: traceHeaders(random, 'checkout-api'),
      };
    },
  },
  {
    name: 'payments',
    partitions: 3,
    replicationFactor: 3,
    records: 640,
    overrides: { 'min.insync.replicas': '2' },
    trafficChance: 0.5,
    generate: (random, sequence) => {
      const paymentId = `pay_${String(50_000 + sequence)}`;
      return {
        key: paymentId,
        value: JSON.stringify({
          paymentId,
          orderId: `ord_${String(10_000 + Math.max(0, sequence - Math.floor(random() * 4)))}`,
          amount: money(random, 12, 480),
          method: pick(random, ['card', 'card', 'card', 'paypal', 'bank-transfer']),
          status: random() < 0.94 ? 'captured' : 'declined',
        }),
        headers: traceHeaders(random, 'payments-service'),
      };
    },
  },
  {
    name: 'inventory.updates',
    partitions: 4,
    replicationFactor: 2,
    records: 820,
    trafficChance: 0.6,
    generate: (random) => {
      const sku = pick(random, SKUS);
      return {
        key: sku,
        value: JSON.stringify({
          sku,
          warehouse: pick(random, ['ams-1', 'fra-2', 'waw-1']),
          delta: random() < 0.7 ? -1 - Math.floor(random() * 3) : 20 + Math.floor(random() * 30),
          onHand: Math.floor(random() * 400),
        }),
        headers: traceHeaders(random, 'warehouse-sync'),
      };
    },
  },
  {
    name: 'customer-profiles',
    partitions: 3,
    replicationFactor: 3,
    records: 140,
    overrides: { 'cleanup.policy': 'compact', 'min.compaction.lag.ms': '3600000' },
    trafficChance: 0.1,
    generate: (random, sequence): ProduceMessage => {
      const customerId = pick(random, CUSTOMERS);
      // A compacted topic: the occasional tombstone shows how Studio renders a deleted key.
      if (sequence > 0 && sequence % 17 === 0) return { key: customerId, value: null, headers: {} };
      return {
        key: customerId,
        value: JSON.stringify({
          customerId,
          tier: pick(random, ['standard', 'standard', 'plus', 'business']),
          country: pick(random, ['NL', 'DE', 'PL', 'UA', 'FR']),
          marketingOptIn: random() < 0.4,
        }),
        headers: { source: 'crm' },
      };
    },
  },
  {
    name: 'audit-log',
    partitions: 1,
    replicationFactor: 2,
    records: 90,
    overrides: { 'retention.ms': '2592000000' },
    trafficChance: 0.15,
    generate: (random) => ({
      value: `${pick(random, ['user.login', 'topic.config.update', 'group.offsets.reset', 'user.logout'])} principal=User:${pick(random, ['alice', 'bob', 'studio', 'ci-bot'])}`,
      headers: { source: 'admin-gateway' },
    }),
  },
];

export interface GroupSeed {
  readonly groupId: string;
  readonly protocolType: 'consumer' | 'share';
  readonly assignorName: string;
  readonly members: readonly GroupMember[];
  /** Topics the group has committed offsets on without a member assigned to them. */
  readonly idleTopics?: readonly string[];
  /** Roughly how far behind the log end the group starts. */
  readonly initialLag: number;
  /** Chance per traffic tick that a member catches up on one assigned partition. */
  readonly consumeChance: number;
}

function member(
  memberId: string,
  clientId: string,
  clientHost: string,
  assignedTopicPartitions: GroupMember['assignedTopicPartitions'],
): GroupMember {
  return { memberId, instanceId: null, clientId, clientHost, assignedTopicPartitions };
}

const orders = (partitions: number[]) => ({ topic: 'orders', partitions });
const payments = (partitions: number[]) => ({ topic: 'payments', partitions });

export const GROUP_SEEDS: readonly GroupSeed[] = [
  {
    groupId: 'order-fulfillment',
    protocolType: 'consumer',
    assignorName: 'range',
    members: [
      member('fulfillment-1-4c1d', 'fulfillment-worker', '/10.0.4.21', [orders([0, 1])]),
      member('fulfillment-2-9a7e', 'fulfillment-worker', '/10.0.4.22', [orders([2, 3])]),
      member('fulfillment-3-e03b', 'fulfillment-worker', '/10.0.4.23', [orders([4, 5])]),
    ],
    initialLag: 3,
    consumeChance: 0.9,
  },
  {
    groupId: 'payment-reconciler',
    protocolType: 'consumer',
    assignorName: 'cooperative-sticky',
    members: [member('reconciler-0f52', 'payment-reconciler', '/10.0.6.10', [payments([0, 1, 2])])],
    initialLag: 40,
    consumeChance: 0.3,
  },
  {
    groupId: 'analytics-loader',
    protocolType: 'consumer',
    assignorName: 'roundrobin',
    members: [
      member('analytics-1-77aa', 'analytics-loader', '/10.0.8.3', [orders([0, 2, 4]), payments([0, 2])]),
      member('analytics-2-31bc', 'analytics-loader', '/10.0.8.4', [orders([1, 3, 5]), payments([1])]),
    ],
    initialLag: 12,
    consumeChance: 0.6,
  },
  // Empty and far behind: a group whose consumers were stopped, ready for an offset reset.
  {
    groupId: 'inventory-sync',
    protocolType: 'consumer',
    assignorName: 'range',
    members: [],
    idleTopics: ['inventory.updates'],
    initialLag: 260,
    consumeChance: 0,
  },
  {
    groupId: 'notification-dispatch',
    protocolType: 'share',
    assignorName: 'simple',
    members: [
      member('share-a1', 'notifier', '/10.0.9.11', [orders([0, 1, 2, 3, 4, 5])]),
      member('share-b2', 'notifier', '/10.0.9.12', [orders([0, 1, 2, 3, 4, 5])]),
    ],
    initialLag: 8,
    consumeChance: 0.8,
  },
];

/** Streams groups (KIP-1071) are read-only in the studio, so the demo serves them as they are. */
export const STREAMS_GROUP_SEEDS: readonly StreamsGroupDetailResponse[] = [
  {
    groupId: 'order-totals',
    state: 'Stable',
    groupEpoch: 7,
    assignorName: 'sticky',
    topologyEpoch: 1,
    subtopologies: [
      {
        subtopologyId: '0',
        sourceTopics: ['orders'],
        repartitionSinkTopics: ['order-totals-by-customer-repartition'],
        stateChangelogTopics: [],
        repartitionSourceTopics: [],
      },
      {
        subtopologyId: '1',
        sourceTopics: [],
        repartitionSinkTopics: [],
        stateChangelogTopics: ['order-totals-totals-store-changelog'],
        repartitionSourceTopics: ['order-totals-by-customer-repartition'],
      },
    ],
    members: [
      {
        memberId: 'streams-a1',
        processId: '3f1c9e2a-6b4d-4f0e-9a8b-1c2d3e4f5a6b',
        clientId: 'order-totals-StreamThread-1',
        clientHost: '/10.0.11.21',
        activeTasks: [
          { subtopologyId: '0', partitions: [0, 1, 2] },
          { subtopologyId: '1', partitions: [0, 1, 2] },
        ],
        standbyTasks: [{ subtopologyId: '1', partitions: [3, 4, 5] }],
        warmupTasks: [],
      },
      {
        memberId: 'streams-b2',
        processId: '8a7b6c5d-4e3f-4a1b-8c9d-0e1f2a3b4c5d',
        clientId: 'order-totals-StreamThread-1',
        clientHost: '/10.0.11.22',
        activeTasks: [
          { subtopologyId: '0', partitions: [3, 4, 5] },
          { subtopologyId: '1', partitions: [3, 4, 5] },
        ],
        standbyTasks: [{ subtopologyId: '1', partitions: [0, 1, 2] }],
        warmupTasks: [],
      },
    ],
  },
];

function allow(resourceType: string, resourceName: string, principal: string, operation: string): AclEntryView {
  return {
    resourceType,
    resourceName,
    resourcePatternType: resourceName.endsWith('.') ? 'PREFIXED' : 'LITERAL',
    principal,
    host: '*',
    operation,
    permissionType: 'ALLOW',
  };
}

export const SAMPLE_ACLS: readonly AclEntryView[] = [
  allow('TOPIC', 'orders', 'User:checkout-api', 'WRITE'),
  allow('TOPIC', 'orders', 'User:fulfillment', 'READ'),
  allow('GROUP', 'order-fulfillment', 'User:fulfillment', 'READ'),
  allow('TOPIC', 'payments', 'User:payments-service', 'WRITE'),
  allow('TOPIC', 'inventory.', 'User:warehouse-sync', 'ALL'),
  { ...allow('TOPIC', 'audit-log', 'User:*', 'WRITE'), permissionType: 'DENY' },
  allow('CLUSTER', 'kafka-cluster', 'User:studio', 'DESCRIBE'),
];

export const SAMPLE_QUOTAS: readonly QuotaEntryView[] = [
  {
    entity: [{ entityType: 'client-id', entityName: 'analytics-loader' }],
    values: [{ key: 'consumer_byte_rate', value: 5_242_880 }],
  },
  {
    entity: [{ entityType: 'user', entityName: 'checkout-api' }],
    values: [
      { key: 'producer_byte_rate', value: 10_485_760 },
      { key: 'request_percentage', value: 50 },
    ],
  },
  { entity: [{ entityType: 'user', entityName: null }], values: [{ key: 'producer_byte_rate', value: 2_097_152 }] },
];

export const SAMPLE_TRANSACTIONS: readonly TransactionListEntry[] = [
  { transactionalId: 'checkout-api-txn-0', producerId: '4001', transactionState: 'Ongoing' },
  { transactionalId: 'checkout-api-txn-1', producerId: '4002', transactionState: 'CompleteCommit' },
  { transactionalId: 'payments-ledger', producerId: '4010', transactionState: 'Empty' },
];

/** One row of the table below: api key, name, the broker's version range (or `null`s), implemented, version used. */
function api(
  apiKey: number,
  apiName: string,
  brokerMinVersion: number | null,
  brokerMaxVersion: number | null,
  isImplemented: boolean,
  version: number | null,
): ApiVersionView {
  const brokerVersions =
    brokerMinVersion === null || brokerMaxVersion === null
      ? null
      : { minVersion: brokerMinVersion, maxVersion: brokerMaxVersion };
  return { apiKey, apiName, brokerVersions, isImplemented, version };
}

/** What a Kafka 4.4 broker advertises, with the versions Studio's client negotiates against it. */
export const SAMPLE_API_VERSIONS: readonly ApiVersionView[] = [
  api(0, 'Produce', 0, 13, true, 13),
  api(1, 'Fetch', 4, 18, true, 18),
  api(2, 'ListOffsets', 1, 11, true, 11),
  api(3, 'Metadata', 0, 13, true, 13),
  api(8, 'OffsetCommit', 2, 10, true, 10),
  api(9, 'OffsetFetch', 1, 10, true, 10),
  api(10, 'FindCoordinator', 0, 6, true, 5),
  api(11, 'JoinGroup', 0, 9, true, 9),
  api(12, 'Heartbeat', 0, 4, true, 4),
  api(13, 'LeaveGroup', 0, 5, true, 5),
  api(14, 'SyncGroup', 0, 5, true, 5),
  api(15, 'DescribeGroups', 0, 6, true, 6),
  api(16, 'ListGroups', 0, 5, true, 5),
  api(17, 'SaslHandshake', 0, 1, true, 1),
  api(18, 'ApiVersions', 0, 5, true, 5),
  api(19, 'CreateTopics', 2, 7, true, 7),
  api(20, 'DeleteTopics', 1, 6, true, 6),
  api(21, 'DeleteRecords', 0, 2, true, 2),
  api(22, 'InitProducerId', 0, 5, true, 5),
  api(23, 'OffsetForLeaderEpoch', 2, 4, true, 4),
  api(24, 'AddPartitionsToTxn', 0, 5, true, 3),
  api(25, 'AddOffsetsToTxn', 0, 4, true, 4),
  api(26, 'EndTxn', 0, 5, true, 5),
  api(27, 'WriteTxnMarkers', 1, 2, true, 2),
  api(28, 'TxnOffsetCommit', 0, 6, true, 6),
  api(29, 'DescribeAcls', 1, 3, true, 3),
  api(30, 'CreateAcls', 1, 3, true, 3),
  api(31, 'DeleteAcls', 1, 3, true, 3),
  api(32, 'DescribeConfigs', 1, 4, true, 4),
  api(33, 'AlterConfigs', 0, 2, true, 2),
  api(34, 'AlterReplicaLogDirs', 1, 2, true, 2),
  api(35, 'DescribeLogDirs', 1, 5, true, 5),
  api(36, 'SaslAuthenticate', 0, 2, true, 2),
  api(37, 'CreatePartitions', 0, 3, true, 3),
  api(38, 'CreateDelegationToken', 1, 3, true, 3),
  api(39, 'RenewDelegationToken', 1, 2, true, 2),
  api(40, 'ExpireDelegationToken', 1, 2, true, 2),
  api(41, 'DescribeDelegationToken', 1, 3, true, 3),
  api(42, 'DeleteGroups', 0, 3, true, 3),
  api(43, 'ElectLeaders', 0, 2, true, 2),
  api(44, 'IncrementalAlterConfigs', 0, 1, true, 1),
  api(45, 'AlterPartitionReassignments', 0, 1, true, 1),
  api(46, 'ListPartitionReassignments', 0, 0, true, 0),
  api(47, 'OffsetDelete', 0, 0, true, 0),
  api(48, 'DescribeClientQuotas', 0, 1, true, 1),
  api(49, 'AlterClientQuotas', 0, 1, true, 1),
  api(50, 'DescribeUserScramCredentials', 0, 0, true, 0),
  api(51, 'AlterUserScramCredentials', 0, 0, true, 0),
  api(55, 'DescribeQuorum', 0, 2, true, 2),
  api(57, 'UpdateFeatures', 0, 2, true, 2),
  api(60, 'DescribeCluster', 0, 2, true, 2),
  api(61, 'DescribeProducers', 0, 0, true, 0),
  api(64, 'UnregisterBroker', 0, 0, true, 0),
  api(65, 'DescribeTransactions', 0, 0, true, 0),
  api(66, 'ListTransactions', 0, 2, true, 2),
  api(68, 'ConsumerGroupHeartbeat', 0, 1, true, 1),
  api(69, 'ConsumerGroupDescribe', 0, 1, true, 1),
  api(71, 'GetTelemetrySubscriptions', null, null, true, null),
  api(72, 'PushTelemetry', null, null, true, null),
  api(73, 'AssignReplicasToDirs', null, null, true, null),
  api(74, 'ListConfigResources', 0, 1, true, 1),
  api(75, 'DescribeTopicPartitions', 0, 0, true, 0),
  api(76, 'ShareGroupHeartbeat', 1, 1, true, 1),
  api(77, 'ShareGroupDescribe', 1, 1, true, 1),
  api(78, 'ShareFetch', 1, 2, true, 2),
  api(79, 'ShareAcknowledge', 1, 2, true, 2),
  api(80, 'AddRaftVoter', 0, 1, true, 1),
  api(81, 'RemoveRaftVoter', 0, 0, true, 0),
  api(83, 'InitializeShareGroupState', 0, 0, false, null),
  api(84, 'ReadShareGroupState', 0, 0, false, null),
  api(85, 'WriteShareGroupState', 0, 1, false, null),
  api(86, 'DeleteShareGroupState', 0, 0, false, null),
  api(87, 'ReadShareGroupStateSummary', 0, 1, false, null),
  api(88, 'StreamsGroupHeartbeat', 0, 1, false, null),
  api(89, 'StreamsGroupDescribe', 0, 1, true, 1),
  api(90, 'DescribeShareGroupOffsets', 0, 1, true, 1),
  api(91, 'AlterShareGroupOffsets', 0, 0, true, 0),
  api(92, 'DeleteShareGroupOffsets', 0, 0, true, 0),
  api(93, 'StreamsGroupTopologyDescriptionUpdate', 0, 0, false, null),
  api(94, 'UnregisterController', 0, 0, true, 0),
];
