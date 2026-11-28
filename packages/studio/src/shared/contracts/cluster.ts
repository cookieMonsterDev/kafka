/** One log dir on one broker, as `GET /api/cluster/log-dirs` reports it. */
export interface LogDirView {
  readonly brokerId: number;
  readonly path: string;
  /**
   * Byte counts are `bigint` on the server and serialize as decimal strings. All three are `null`
   * when the broker does not report volume sizes (DescribeLogDirs v3 or older) or cannot read them.
   */
  readonly totalBytes: string | null;
  readonly usableBytes: string | null;
  readonly usedBytes: string | null;
  /** `null` when the broker does not report cordoning (DescribeLogDirs v4 or older). */
  readonly isCordoned: boolean | null;
}

export interface LogDirListResponse {
  /** Sorted by broker id, then path. */
  readonly logDirs: readonly LogDirView[];
}

/** One Kafka API on one broker, as `GET /api/cluster/api-versions` reports it. */
export interface ApiVersionView {
  readonly apiKey: number;
  /** `null` for an api key Studio's client cannot name. */
  readonly apiName: string | null;
  /** The versions the broker advertises; `null` when it does not support the API. */
  readonly brokerVersions: { readonly minVersion: number; readonly maxVersion: number } | null;
  readonly isImplemented: boolean;
  /** The version Studio's client negotiates; `null` when either side lacks the API or they share no version. */
  readonly version: number | null;
}

/** One broker's Kafka APIs. */
export interface BrokerApiVersionsView {
  readonly nodeId: number;
  readonly host: string;
  readonly port: number;
  readonly rack: string | null;
  /** In api key order. */
  readonly apis: readonly ApiVersionView[];
}

export interface ApiVersionsResponse {
  /** Sorted by node id. */
  readonly brokers: readonly BrokerApiVersionsView[];
}
