/** The demo cluster's starting state and sample records. Seeded, so every visit opens on the same data. */

import type { GroupMember } from '../../../shared/contracts/group';
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
