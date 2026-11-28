import { AddOffsetsToTxn } from './add-offsets-to-txn/index';
import { AddPartitionsToTxn } from './add-partitions-to-txn/index';
import { ApiVersions } from './api-versions/index';
import { ConsumerGroupHeartbeat } from './consumer-group-heartbeat/index';
import { DescribeGroups } from './describe-groups/index';
import { EndTxn } from './end-txn/index';
import { Fetch } from './fetch/index';
import { FindCoordinator } from './find-coordinator/index';
import { GetTelemetrySubscriptions } from './get-telemetry-subscriptions/index';
import { Heartbeat } from './heartbeat/index';
import type { RequestFamily } from './index';
import { InitProducerId } from './init-producer-id/index';
import { JoinGroup } from './join-group/index';
import { LeaveGroup } from './leave-group/index';
import { ListOffsets } from './list-offsets/index';
import { Metadata } from './metadata/index';
import { OffsetCommit } from './offset-commit/index';
import { OffsetFetch } from './offset-fetch/index';
import { OffsetForLeaderEpoch } from './offset-for-leader-epoch/index';
import { Produce } from './produce/index';
import { PushTelemetry } from './push-telemetry/index';
import { SaslAuthenticate } from './sasl-authenticate/index';
import { SaslHandshake } from './sasl-handshake/index';
import { ShareAcknowledge } from './share-acknowledge/index';
import { ShareFetch } from './share-fetch/index';
import { ShareGroupHeartbeat } from './share-group-heartbeat/index';
import { SyncGroup } from './sync-group/index';
import { TxnOffsetCommit } from './txn-offset-commit/index';

/** What a request family states about the versions of its Kafka API this client implements. */
export type ImplementedApi = Pick<RequestFamily, 'apiKey' | 'versions'>;

/** The families producers, consumers and connections load at startup; the rest are in `./lazy-requests`. */
const EAGER_FAMILIES: readonly ImplementedApi[] = [
  Produce,
  Fetch,
  ListOffsets,
  Metadata,
  OffsetCommit,
  OffsetFetch,
  FindCoordinator,
  JoinGroup,
  Heartbeat,
  LeaveGroup,
  SyncGroup,
  DescribeGroups,
  SaslHandshake,
  ApiVersions,
  InitProducerId,
  OffsetForLeaderEpoch,
  AddPartitionsToTxn,
  AddOffsetsToTxn,
  EndTxn,
  TxnOffsetCommit,
  SaslAuthenticate,
  ConsumerGroupHeartbeat,
  GetTelemetrySubscriptions,
  PushTelemetry,
  ShareGroupHeartbeat,
  ShareFetch,
  ShareAcknowledge,
];

/** One family per Kafka API this client implements, loading the lazy admin families first. */
export async function loadImplementedApis(): Promise<ImplementedApi[]> {
  // The same chunk the stubs in `./lazy-families` import on first use; a static import would put
  // its codecs in the startup graph.
  const lazy: ImplementedApi[] = Object.values(await import('./lazy-requests'));
  return [...EAGER_FAMILIES, ...lazy];
}
