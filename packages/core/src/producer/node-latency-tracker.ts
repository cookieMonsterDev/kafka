/** Weight given to the newest sample in the exponential moving average. */
const EWMA_ALPHA = 0.3;

/** Read-only view a partitioner uses to bias partition choice toward faster leaders. */
export interface NodeLatencyReader {
  /** EWMA Produce latency for this node, in ms. `undefined` if never measured. */
  latencyFor(nodeId: number): number | undefined;
}

export interface NodeLatencyTracker extends NodeLatencyReader {
  /** Folds one more successful Produce round-trip into that node's running average. */
  record(nodeId: number, durationMs: number): void;
}

/**
 * Per-node Produce latency, tracked as an exponential moving average so a handful of recent
 * round-trips can shift the estimate without one outlier dominating it. Shared across a
 * producer's whole lifetime (including transactions) - it's about how fast a broker node
 * responds, not anything specific to one send.
 */
export function createNodeLatencyTracker(): NodeLatencyTracker {
  const latencyByNode = new Map<number, number>();

  return {
    record(nodeId, durationMs) {
      const previous = latencyByNode.get(nodeId);
      latencyByNode.set(nodeId, previous == null ? durationMs : previous + EWMA_ALPHA * (durationMs - previous));
    },
    latencyFor(nodeId) {
      return latencyByNode.get(nodeId);
    },
  };
}
