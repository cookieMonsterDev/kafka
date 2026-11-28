import type { FinalizedFeatureLevels } from '../protocol/requests/api-versions/feature-fields';
import { API_KEYS } from '../protocol/requests/api-keys';
import type { BrokerVersions } from '../protocol/requests/index';

/** The finalized feature that sets the transaction protocol version (KIP-890). */
const TRANSACTION_VERSION_FEATURE = 'transaction.version';

function advertised(versions: BrokerVersions, apiKey: number) {
  return versions[apiKey];
}

/** RecordBatch (magic 2) is available from Produce v3 / Kafka 0.11. */
export function supportsRecordBatch(versions: BrokerVersions): boolean {
  const produce = advertised(versions, API_KEYS.Produce);
  return produce != null && produce.maxVersion >= 3;
}

/** Headers travel inside RecordBatch; same floor as `supportsRecordBatch`. */
export function supportsHeaders(versions: BrokerVersions): boolean {
  return supportsRecordBatch(versions);
}

/** Transactions require InitProducerId (Kafka 0.11+). */
export function supportsTransactions(versions: BrokerVersions): boolean {
  const initProducerId = advertised(versions, API_KEYS.InitProducerId);
  return initProducerId != null && initProducerId.maxVersion != null;
}

/** ZSTD compression is negotiated from Produce v7 (Kafka 2.1). */
export function supportsZstd(versions: BrokerVersions): boolean {
  const produce = advertised(versions, API_KEYS.Produce);
  return produce != null && produce.maxVersion >= 7;
}

/** ACL resource pattern type (`LITERAL` / `PREFIXED`) arrives on DescribeAcls v1 (Kafka 2.0). */
export function supportsAclPatternType(versions: BrokerVersions): boolean {
  const describeAcls = advertised(versions, API_KEYS.DescribeAcls);
  return describeAcls != null && describeAcls.maxVersion >= 1;
}

/**
 * KIP-890 part 2: transaction version 2 is on once the cluster finalizes `transaction.version` at 2
 * or higher (Kafka 4.0+). Brokers advertise its request versions either way, so the feature level
 * decides, as in the Java client.
 */
export function supportsTransactionV2(finalizedFeatures: FinalizedFeatureLevels): boolean {
  return (finalizedFeatures[TRANSACTION_VERSION_FEATURE] ?? 0) >= 2;
}

/**
 * KIP-919: DescribeCluster v1+ can request controller endpoints (`endpointType=CONTROLLER`).
 * Kafka 3.7+.
 */
export function supportsDescribeClusterControllers(versions: BrokerVersions): boolean {
  const describeCluster = advertised(versions, API_KEYS.DescribeCluster);
  return describeCluster != null && describeCluster.maxVersion >= 1;
}

/** KIP-714 client telemetry: GetTelemetrySubscriptions (71) / PushTelemetry (72). Kafka 3.5+. */
export function supportsClientTelemetry(versions: BrokerVersions): boolean {
  const subscriptions = advertised(versions, API_KEYS.GetTelemetrySubscriptions);
  return subscriptions != null && subscriptions.maxVersion != null;
}

/** KIP-699: FindCoordinator v4 looks up several coordinator keys in one request. Kafka 3.0+. */
export function supportsBatchedFindCoordinator(versions: BrokerVersions): boolean {
  const findCoordinator = advertised(versions, API_KEYS.GroupCoordinator);
  return findCoordinator != null && findCoordinator.maxVersion >= 4;
}

/** KIP-709: OffsetFetch v8 fetches the committed offsets of several groups in one request. Kafka 3.0+. */
export function supportsBatchedOffsetFetch(versions: BrokerVersions): boolean {
  const offsetFetch = advertised(versions, API_KEYS.OffsetFetch);
  return offsetFetch != null && offsetFetch.maxVersion >= 8;
}

/**
 * KIP-1242: ApiVersions v5 carries the cluster id and node id the client expects, so a broker
 * reached through a reused address can answer `REBOOTSTRAP_REQUIRED`. Kafka 4.4+.
 */
export function supportsMisroutedConnectionCheck(versions: BrokerVersions): boolean {
  const apiVersions = advertised(versions, API_KEYS.ApiVersions);
  return apiVersions != null && apiVersions.maxVersion >= 5;
}
