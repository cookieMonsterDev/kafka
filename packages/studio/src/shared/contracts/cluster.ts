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
