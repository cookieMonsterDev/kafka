import type { ApiVersionsResponse, LogDirListResponse } from '../../../shared/contracts/cluster';
import type { DebugRecord, ScanCoverage, ScanRequest, ScanResult } from '../../../shared/contracts/debug';
import type { StudioEvent } from '../../../shared/contracts/event';
import type {
  DeleteGroupOffsetsRequest,
  DeleteGroupOffsetsResponse,
  GroupDetailResponse,
  GroupListResponse,
  GroupMember,
  RemoveGroupMembersRequest,
  RemoveGroupMembersResponse,
  ResetGroupOffsetsRequest,
  ResetGroupOffsetsResponse,
  ShareGroupDetailResponse,
  ShareGroupListResponse,
  StreamsGroupDetailResponse,
  StreamsGroupListResponse,
} from '../../../shared/contracts/group';
import type {
  DeleteRecordsRequest,
  MessageRecord,
  MessagesPageResponse,
  MessagesQuery,
} from '../../../shared/contracts/message';
import type { ProduceMessage, ProduceRequest, ProduceResponse } from '../../../shared/contracts/produce';
import type {
  AlterTopicConfigsRequest,
  CreateTopicRequest,
  TopicConfigEntry,
  TopicDetailResponse,
  TopicListResponse,
} from '../../../shared/contracts/topic';
import { matchesPredicates } from '../../../shared/message-query';
// Browser-safe: the bus only imports a contract type. Sharing it keeps the demo's board feed identical to the server's.
import { StudioEventBus, type StudioEventInput } from '../../../server/kafka/events';
import { encodeText } from '../../lib/debug-api';
import {
  GROUP_SEEDS,
  SAMPLE_API_VERSIONS,
  STREAMS_GROUP_SEEDS,
  TOPIC_SEEDS,
  createRandom,
  type GroupSeed,
  type RandomSource,
  type TopicSeed,
} from './sample-data';

/** A failed demo request. The route layer turns it into the server's `{ error: { code, message } }` envelope. */
export class DemoError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'DemoError';
    this.status = status;
    this.code = code;
  }
}

interface StoredRecord {
  readonly partition: number;
  readonly offset: number;
  readonly timestamp: number;
  /** Stored as text; base64 only at the API boundary, where the real server encodes raw bytes. */
  readonly key: string | null;
  readonly value: string | null;
  readonly headers: Readonly<Record<string, string>>;
  readonly size: number;
}

interface DemoPartition {
  readonly index: number;
  readonly replicas: readonly number[];
  /** The first retained offset. Advances when retention drops old records or when records are deleted. */
  logStart: number;
  readonly records: StoredRecord[];
}

interface DemoTopic {
  readonly name: string;
  readonly partitions: DemoPartition[];
  readonly overrides: Map<string, string>;
  roundRobin: number;
}

interface DemoGroup {
  readonly groupId: string;
  readonly protocolType: GroupSeed['protocolType'];
  readonly assignorName: string;
  readonly consumeChance: number;
  members: readonly GroupMember[];
  /** Committed offset (or share-group start offset) by topic, then partition. */
  readonly committed: Map<string, Map<number, number>>;
}

type RecordListener = (topic: string, record: MessageRecord) => void;

const SEED = 20_260_101;
const BROKER_IDS = [1, 2, 3] as const;
/** Bounds memory in a long-open tab: older records age out like time-based retention would drop them. */
const MAX_RECORDS_PER_PARTITION = 1_500;
const SEED_WINDOW_MS = 6 * 60 * 60 * 1000;
/** Per-record overhead in a v2 record batch, so partition sizes read like a broker's log dirs. */
const RECORD_OVERHEAD_BYTES = 70;
const GIB = 1024 ** 3;
/** Kafka's per-member error for a member id the group doesn't know. */
const UNKNOWN_MEMBER_ID = 25;

/** Every topic config the demo reports, with Kafka's own default value. */
const TOPIC_CONFIG_DEFAULTS: Readonly<Record<string, string>> = {
  'cleanup.policy': 'delete',
  'compression.type': 'producer',
  'delete.retention.ms': '86400000',
  'file.delete.delay.ms': '60000',
  'max.compaction.lag.ms': '9223372036854775807',
  'max.message.bytes': '1048588',
  'message.timestamp.type': 'CreateTime',
  'min.cleanable.dirty.ratio': '0.5',
  'min.compaction.lag.ms': '0',
  'min.insync.replicas': '1',
  'retention.bytes': '-1',
  'retention.ms': '604800000',
  'segment.bytes': '1073741824',
  'segment.ms': '604800000',
  'unclean.leader.election.enable': 'false',
};

const textEncoder = new TextEncoder();

function byteLength(value: string | null | undefined): number {
  return value === null || value === undefined ? 0 : textEncoder.encode(value).byteLength;
}

function messageBytes(message: ProduceMessage): number {
  let size = byteLength(message.key) + byteLength(message.value);
  for (const [name, value] of Object.entries(message.headers ?? {})) size += byteLength(name) + byteLength(value);
  return size;
}

function encodeNullable(value: string | null): string | null {
  return value === null ? null : encodeText(value);
}

function toMessageRecord(record: StoredRecord): MessageRecord {
  return {
    partition: record.partition,
    offset: String(record.offset),
    timestamp: String(record.timestamp),
    key: encodeNullable(record.key),
    value: encodeNullable(record.value),
    headers: Object.fromEntries(Object.entries(record.headers).map(([name, value]) => [name, encodeText(value)])),
    size: record.size,
  };
}

/** FNV-1a. Stable key-to-partition placement for the demo; not Kafka's murmur2 partitioner. */
function hashKey(key: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < key.length; index++) {
    hash ^= key.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash;
}

function highWatermark(partition: DemoPartition): number {
  return partition.logStart + partition.records.length;
}

/** The first offset whose record is at or after `timestamp`, or the high watermark when none is. */
function offsetForTimestamp(partition: DemoPartition, timestamp: number): number {
  return partition.records.find((record) => record.timestamp >= timestamp)?.offset ?? highWatermark(partition);
}

function requirePartition(topic: DemoTopic, index: number): DemoPartition {
  const partition = topic.partitions[index];
  if (partition === undefined) {
    throw new DemoError(400, 'unknown_partition', `topic "${topic.name}" has no partition ${String(index)}`);
  }
  return partition;
}

function groupState(group: DemoGroup): string {
  return group.members.length > 0 ? 'Stable' : 'Empty';
}

/**
 * The whole fake cluster behind the docs demo: three brokers, a few topics with sample records,
 * and consumer groups whose lag moves with background traffic. Every
 * mutation the Studio UI can make is applied here, so the demo responds the way a real cluster would.
 */
export class DemoCluster {
  readonly clusterId = 'demo-k4fK4StUd10Cl5tR';
  readonly startedAt = Date.now();

  /** The board's activity feed: like the real server, only traffic Studio itself produced or tailed. */
  private readonly activity = new StudioEventBus();
  private readonly topics = new Map<string, DemoTopic>();
  private readonly groups = new Map<string, DemoGroup>();
  private readonly recordListeners = new Set<RecordListener>();
  private readonly random: RandomSource = createRandom(SEED);
  private readonly sequences = new Map<string, number>();

  constructor() {
    const now = Date.now();
    for (const seed of TOPIC_SEEDS) this.seedTopic(seed, now);
    for (const seed of GROUP_SEEDS) this.seedGroup(seed);
  }

  // --- Cluster ---------------------------------------------------------------------------------

  brokerCount(): number {
    return BROKER_IDS.length;
  }

  logDirs(): LogDirListResponse {
    return {
      logDirs: BROKER_IDS.map((brokerId, index) => {
        const total = 500 * GIB;
        const used = (118 + index * 23) * GIB;
        return {
          brokerId,
          path: '/var/lib/kafka/data',
          totalBytes: String(total),
          usableBytes: String(total - used),
          usedBytes: String(used),
          isCordoned: false,
        };
      }),
    };
  }

  apiVersions(): ApiVersionsResponse {
    return {
      brokers: BROKER_IDS.map((nodeId, index) => ({
        nodeId,
        host: 'localhost',
        port: 9092 + index,
        rack: `rack-${String.fromCharCode(97 + index)}`,
        apis: SAMPLE_API_VERSIONS,
      })),
    };
  }

  // --- Topics ----------------------------------------------------------------------------------

  listTopics(): TopicListResponse {
    return {
      topics: [...this.topics.values()]
        .map((topic) => ({
          name: topic.name,
          partitionCount: topic.partitions.length,
          replicationFactor: topic.partitions[0]?.replicas.length ?? null,
        }))
        .sort((a, b) => a.name.localeCompare(b.name)),
    };
  }

  assertTopicExists(name: string): void {
    this.requireTopic(name);
  }

  topicDetail(name: string): TopicDetailResponse {
    const topic = this.requireTopic(name);
    const configs: TopicConfigEntry[] = Object.entries(TOPIC_CONFIG_DEFAULTS).map(([configName, defaultValue]) => {
      const override = topic.overrides.get(configName);
      return {
        name: configName,
        value: override ?? defaultValue,
        isDefault: override === undefined,
        readOnly: false,
        isSensitive: false,
      };
    });
    return {
      name,
      partitions: topic.partitions.map((partition) => ({
        partitionIndex: partition.index,
        leader: partition.replicas[0] ?? BROKER_IDS[0],
        replicas: partition.replicas,
        isr: partition.replicas,
        earliestOffset: String(partition.logStart),
        latestOffset: String(highWatermark(partition)),
        sizeBytes: String(partition.records.reduce((total, record) => total + record.size + RECORD_OVERHEAD_BYTES, 0)),
      })),
      configs,
    };
  }

  createTopic(request: CreateTopicRequest): void {
    if (this.topics.has(request.topic)) {
      throw new DemoError(409, 'topic_already_exists', `topic "${request.topic}" already exists`);
    }
    const replicationFactor = request.replicationFactor ?? 1;
    if (replicationFactor > BROKER_IDS.length) {
      throw new DemoError(
        400,
        'invalid_replication_factor',
        `replication factor ${String(replicationFactor)} is larger than the ${String(BROKER_IDS.length)} available brokers`,
      );
    }
    const overrides = request.configEntries ?? {};
    for (const configName of Object.keys(overrides)) this.requireKnownConfig(configName);
    this.addTopic({
      name: request.topic,
      partitions: request.numPartitions ?? 1,
      replicationFactor,
      overrides,
      maxLogStart: 0,
    });
  }

  deleteTopic(name: string): void {
    this.requireTopic(name);
    this.topics.delete(name);
    for (const group of this.groups.values()) {
      group.committed.delete(name);
      group.members = group.members.map((entry) => ({
        ...entry,
        assignedTopicPartitions: entry.assignedTopicPartitions.filter((assigned) => assigned.topic !== name),
      }));
    }
  }

  addPartitions(name: string, count: number): void {
    const topic = this.requireTopic(name);
    if (count <= topic.partitions.length) {
      throw new DemoError(
        400,
        'invalid_partitions',
        `topic "${name}" already has ${String(topic.partitions.length)} partitions; the new count must be larger`,
      );
    }
    const replicationFactor = topic.partitions[0]?.replicas.length ?? 1;
    for (let index = topic.partitions.length; index < count; index++) {
      topic.partitions.push(this.createPartition(index, replicationFactor, 0));
    }
  }

  alterConfigs(name: string, request: AlterTopicConfigsRequest): void {
    const topic = this.requireTopic(name);
    const set = Object.entries(request.set ?? {});
    const unset = request.unset ?? [];
    for (const configName of [...set.map(([configName]) => configName), ...unset]) this.requireKnownConfig(configName);
    for (const [configName, value] of set) topic.overrides.set(configName, value);
    for (const configName of unset) topic.overrides.delete(configName);
  }

  // --- Messages --------------------------------------------------------------------------------

  /** Same page semantics as `readMessagesPage` on the server: a seek reads forward, no seek reads the newest records. */
  readMessages(name: string, query: MessagesQuery): MessagesPageResponse {
    const topic = this.requireTopic(name);
    const ranges = topic.partitions.map((partition) => ({
      partition: partition.index,
      low: String(partition.logStart),
      high: String(highWatermark(partition)),
    }));
    const targets =
      query.partition === undefined
        ? topic.partitions
        : topic.partitions.filter((partition) => partition.index === query.partition);
    const isTailRead = query.from === undefined && query.timestamp === undefined;

    const collected: StoredRecord[] = [];
    for (const partition of targets) {
      const high = highWatermark(partition);
      let start: number;
      if (query.timestamp !== undefined) start = offsetForTimestamp(partition, query.timestamp);
      else if (query.from === 'earliest') start = partition.logStart;
      else if (query.from === 'latest') start = high;
      else if (query.from !== undefined) start = Math.max(partition.logStart, Number(query.from));
      else start = Math.max(partition.logStart, high - query.limit);
      const first = start - partition.logStart;
      collected.push(...partition.records.slice(first, first + query.limit));
    }

    collected.sort((a, b) => a.partition - b.partition || a.offset - b.offset);
    const limited = isTailRead ? collected.slice(-query.limit) : collected.slice(0, query.limit);
    return { messages: limited.map(toMessageRecord), ranges };
  }

  deleteRecords(name: string, request: DeleteRecordsRequest): void {
    const topic = this.requireTopic(name);
    for (const target of request.partitions) {
      const partition = requirePartition(topic, target.partition);
      const before = Math.min(Number(target.beforeOffset), highWatermark(partition));
      const dropped = Math.max(0, before - partition.logStart);
      partition.records.splice(0, dropped);
      partition.logStart += dropped;
    }
  }

  /** `origin: 'studio'` traffic appears on the board, matching the real server, which only reports its own produce and tail activity. */
  produce(request: ProduceRequest, origin: 'studio' | 'background'): ProduceResponse {
    const topic = this.requireTopic(request.topic);
    const records = request.messages.map((message) => this.append(topic, message));
    const baseOffsets = new Map<number, number>();
    for (const record of records) {
      if (!baseOffsets.has(record.partition)) baseOffsets.set(record.partition, record.offset);
    }
    if (origin === 'studio') {
      const partitions = [...baseOffsets.keys()];
      this.publishActivity({
        kind: 'produce',
        topic: topic.name,
        partition: partitions.length === 1 ? (partitions[0] ?? null) : null,
        count: records.length,
        bytes: records.reduce((total, record) => total + record.size, 0),
      });
    }
    // After the produce activity, as on the real server, where a tail only sees a record once the write is acknowledged.
    if (this.recordListeners.size > 0) {
      for (const record of records) {
        const wire = toMessageRecord(record);
        for (const listener of this.recordListeners) listener(topic.name, wire);
      }
    }
    return {
      results: [...baseOffsets].map(([partition, offset]) => ({ partition, offset: String(offset) })),
    };
  }

  /** Stand-in for `POST /api/debug/scan`: searches the retained records, honoring the same limits. */
  scan(request: ScanRequest): ScanResult {
    const records: DebugRecord[] = [];
    const coverage: ScanCoverage[] = [];
    let scanned = 0;
    let matched = 0;
    let reason: ScanResult['reason'] = 'complete';

    for (const name of request.topics) {
      const topic = this.requireTopic(name);
      for (const partition of topic.partitions) {
        const high = highWatermark(partition);
        let next = partition.logStart;
        for (const record of partition.records) {
          if (reason !== 'complete') break;
          if (scanned >= request.maxRecords) {
            reason = 'record-limit';
            break;
          }
          scanned++;
          next = record.offset + 1;
          const inTime =
            (request.startTime === undefined || record.timestamp >= request.startTime) &&
            (request.endTime === undefined || record.timestamp <= request.endTime);
          const searchable = { ...record, offset: String(record.offset), timestamp: String(record.timestamp) };
          if (!inTime || !matchesPredicates(searchable, request.predicates)) continue;
          matched++;
          records.push({ ...toMessageRecord(record), topic: name });
          if (records.length >= request.maxResults) reason = 'result-limit';
        }
        coverage.push({
          topic: name,
          partition: partition.index,
          start: String(partition.logStart),
          end: String(high),
          next: String(next),
          complete: next >= high,
        });
      }
    }

    return {
      records,
      scanned,
      matched,
      complete: reason === 'complete' && coverage.every((entry) => entry.complete),
      reason,
      coverage,
    };
  }

  // --- Consumer groups -------------------------------------------------------------------------

  listGroups(): GroupListResponse {
    return { groups: this.groupListings('consumer') };
  }

  listShareGroups(): ShareGroupListResponse {
    return { groups: this.groupListings('share') };
  }

  listStreamsGroups(): StreamsGroupListResponse {
    return { groups: STREAMS_GROUP_SEEDS.map(({ groupId }) => ({ groupId, protocolType: 'streams' })) };
  }

  streamsGroupDetail(groupId: string): StreamsGroupDetailResponse {
    const group = STREAMS_GROUP_SEEDS.find((seed) => seed.groupId === groupId);
    if (group === undefined) throw new DemoError(404, 'unknown_group', `streams group "${groupId}" not found`);
    return group;
  }

  groupDetail(groupId: string): GroupDetailResponse {
    const group = this.requireGroup(groupId, 'consumer');
    return {
      groupId,
      state: groupState(group),
      protocolType: 'consumer',
      assignorName: group.members.length > 0 ? group.assignorName : '',
      members: group.members,
      partitionLag: [...group.committed].flatMap(([topic, partitions]) =>
        [...partitions].map(([partition, committedOffset]) => {
          const logEndOffset = this.logEndOffset(topic, partition);
          return {
            topic,
            partition,
            committedOffset: String(committedOffset),
            logEndOffset: String(logEndOffset),
            lag: String(Math.max(0, logEndOffset - committedOffset)),
          };
        }),
      ),
    };
  }

  shareGroupDetail(groupId: string): ShareGroupDetailResponse {
    const group = this.requireGroup(groupId, 'share');
    return {
      groupId,
      state: groupState(group),
      members: group.members,
      offsets: [...group.committed].map(([topic, partitions]) => ({
        topic,
        partitions: [...partitions].map(([partition, startOffset]) => ({
          partition,
          startOffset: String(startOffset),
          lag: String(Math.max(0, this.logEndOffset(topic, partition) - startOffset)),
        })),
      })),
    };
  }

  resetOffsets(groupId: string, request: ResetGroupOffsetsRequest): ResetGroupOffsetsResponse {
    const group = this.requireGroup(groupId, 'consumer');
    if (group.members.length > 0) {
      throw new DemoError(
        409,
        'group_not_empty',
        `group "${groupId}" has active members; stop its consumers (or remove the members) before resetting offsets`,
      );
    }
    const topic = this.requireTopic(request.topic);
    const committed = this.committedOffsets(group, topic.name);
    const partitions = request.partitions.map((target) => {
      const partition = requirePartition(topic, target.partition);
      let offset: number;
      if (target.to === 'earliest') offset = partition.logStart;
      else if (target.to === 'latest') offset = highWatermark(partition);
      else if (target.to === 'offset') offset = Number(target.offset);
      else offset = offsetForTimestamp(partition, target.timestamp);
      committed.set(partition.index, offset);
      return { partition: partition.index, offset: String(offset) };
    });
    return { groupId, topic: topic.name, partitions };
  }

  deleteOffsets(groupId: string, request: DeleteGroupOffsetsRequest): DeleteGroupOffsetsResponse {
    const group = this.requireGroup(groupId, 'consumer');
    return {
      groupId,
      topics: request.topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map((partition) => {
          group.committed.get(entry.topic)?.delete(partition);
          return { partition, errorCode: 0 };
        }),
      })),
    };
  }

  removeMembers(groupId: string, request: RemoveGroupMembersRequest): RemoveGroupMembersResponse {
    const group = this.requireGroup(groupId, 'consumer');
    const known = new Set(group.members.map((entry) => entry.memberId));
    const removed = new Set(request.members.map((target) => target.memberId));
    group.members = group.members.filter((entry) => !removed.has(entry.memberId));
    return {
      groupId,
      members: request.members.map((target) => ({
        memberId: target.memberId,
        groupInstanceId: target.groupInstanceId ?? null,
        errorCode: known.has(target.memberId) ? 0 : UNKNOWN_MEMBER_ID,
      })),
    };
  }

  deleteGroup(groupId: string): void {
    const group = this.requireGroup(groupId, 'consumer');
    if (group.members.length > 0) {
      throw new DemoError(409, 'group_not_empty', `could not delete group "${groupId}": it still has active members`);
    }
    this.groups.delete(groupId);
  }

  // --- Live activity ---------------------------------------------------------------------------

  onRecord(listener: RecordListener): () => void {
    this.recordListeners.add(listener);
    return () => this.recordListeners.delete(listener);
  }

  onActivity(listener: (event: StudioEvent) => void): () => void {
    return this.activity.subscribe(listener);
  }

  publishActivity(event: StudioEventInput): void {
    this.activity.publish(event);
  }

  /** One step of background traffic: other applications producing and consuming, so tails and lag move. */
  tick(): void {
    for (const seed of TOPIC_SEEDS) {
      if (!this.topics.has(seed.name) || this.random() >= seed.trafficChance) continue;
      this.produce({ topic: seed.name, messages: [this.nextSample(seed)] }, 'background');
    }
    for (const group of this.groups.values()) {
      for (const entry of group.members) {
        for (const assigned of entry.assignedTopicPartitions) {
          const committed = this.committedOffsets(group, assigned.topic);
          for (const partition of assigned.partitions) {
            if (this.random() >= group.consumeChance) continue;
            const offset = committed.get(partition);
            const end = this.logEndOffset(assigned.topic, partition);
            if (offset !== undefined && offset < end) committed.set(partition, Math.min(end, offset + 2));
          }
        }
      }
    }
  }

  // --- Internals -------------------------------------------------------------------------------

  private requireTopic(name: string): DemoTopic {
    const topic = this.topics.get(name);
    if (topic === undefined) throw new DemoError(404, 'unknown_topic', `topic "${name}" does not exist`);
    return topic;
  }

  private requireGroup(groupId: string, protocolType: DemoGroup['protocolType']): DemoGroup {
    const group = this.groups.get(groupId);
    if (group === undefined || group.protocolType !== protocolType) {
      const label = protocolType === 'share' ? 'share group' : 'group';
      throw new DemoError(404, 'unknown_group', `${label} "${groupId}" not found`);
    }
    return group;
  }

  private requireKnownConfig(configName: string): void {
    if (!Object.hasOwn(TOPIC_CONFIG_DEFAULTS, configName)) {
      throw new DemoError(400, 'invalid_config', `unknown topic config "${configName}"`);
    }
  }

  private committedOffsets(group: DemoGroup, topic: string): Map<number, number> {
    let offsets = group.committed.get(topic);
    if (offsets === undefined) {
      offsets = new Map();
      group.committed.set(topic, offsets);
    }
    return offsets;
  }

  private groupListings(protocolType: DemoGroup['protocolType']) {
    return [...this.groups.values()]
      .filter((group) => group.protocolType === protocolType)
      .map((group) => ({ groupId: group.groupId, protocolType }))
      .sort((a, b) => a.groupId.localeCompare(b.groupId));
  }

  private logEndOffset(topicName: string, partitionIndex: number): number {
    const partition = this.topics.get(topicName)?.partitions[partitionIndex];
    return partition === undefined ? 0 : highWatermark(partition);
  }

  private createPartition(index: number, replicationFactor: number, logStart: number): DemoPartition {
    const replicas = Array.from(
      { length: replicationFactor },
      (_, offset) => BROKER_IDS[(index + offset) % BROKER_IDS.length] ?? BROKER_IDS[0],
    );
    return { index, replicas, logStart, records: [] };
  }

  private addTopic(options: {
    readonly name: string;
    readonly partitions: number;
    readonly replicationFactor: number;
    readonly overrides: Readonly<Record<string, string>>;
    /** Upper bound for a random first offset, as if retention had already dropped older records. */
    readonly maxLogStart: number;
  }): DemoTopic {
    const { name, replicationFactor, maxLogStart } = options;
    const topic: DemoTopic = {
      name,
      partitions: Array.from({ length: options.partitions }, (_, index) =>
        this.createPartition(index, replicationFactor, Math.floor(this.random() * maxLogStart)),
      ),
      overrides: new Map(Object.entries(options.overrides)),
      roundRobin: 0,
    };
    this.topics.set(name, topic);
    return topic;
  }

  private seedTopic(seed: TopicSeed, now: number): void {
    // A long-lived, non-compacted topic has already had older records removed by retention.
    const maxLogStart = seed.overrides?.['cleanup.policy'] === 'compact' ? 0 : 40_000;
    const topic = this.addTopic({ ...seed, overrides: seed.overrides ?? {}, maxLogStart });
    for (let index = 0; index < seed.records; index++) {
      const timestamp = now - SEED_WINDOW_MS + Math.floor((index / seed.records) * SEED_WINDOW_MS);
      this.append(topic, { ...this.nextSample(seed), timestamp });
    }
  }

  private seedGroup(seed: GroupSeed): void {
    const topics = new Set(seed.idleTopics);
    for (const entry of seed.members) for (const assigned of entry.assignedTopicPartitions) topics.add(assigned.topic);
    const committed = new Map<string, Map<number, number>>();
    for (const name of topics) {
      const offsets = new Map<number, number>();
      for (const partition of this.topics.get(name)?.partitions ?? []) {
        const spread = Math.floor(this.random() * Math.max(1, seed.initialLag / 2));
        offsets.set(partition.index, Math.max(partition.logStart, highWatermark(partition) - seed.initialLag + spread));
      }
      committed.set(name, offsets);
    }
    this.groups.set(seed.groupId, {
      groupId: seed.groupId,
      protocolType: seed.protocolType,
      assignorName: seed.assignorName,
      consumeChance: seed.consumeChance,
      members: seed.members,
      committed,
    });
  }

  private nextSample(seed: TopicSeed): ProduceMessage {
    const sequence = this.sequences.get(seed.name) ?? 0;
    this.sequences.set(seed.name, sequence + 1);
    return seed.generate(this.random, sequence);
  }

  private choosePartition(topic: DemoTopic, message: ProduceMessage): DemoPartition {
    if (message.partition !== undefined) return requirePartition(topic, message.partition);
    if (message.key !== undefined) return requirePartition(topic, hashKey(message.key) % topic.partitions.length);
    const index = topic.roundRobin % topic.partitions.length;
    topic.roundRobin++;
    return requirePartition(topic, index);
  }

  private append(topic: DemoTopic, message: ProduceMessage): StoredRecord {
    const partition = this.choosePartition(topic, message);
    const record: StoredRecord = {
      partition: partition.index,
      offset: highWatermark(partition),
      timestamp: message.timestamp ?? Date.now(),
      key: message.key ?? null,
      value: message.value,
      headers: message.headers ?? {},
      size: messageBytes(message),
    };
    partition.records.push(record);
    if (partition.records.length > MAX_RECORDS_PER_PARTITION) {
      partition.records.shift();
      partition.logStart++;
    }
    return record;
  }
}
