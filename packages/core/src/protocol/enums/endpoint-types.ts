/**
 * DescribeCluster `endpoint_type` (KIP-919).
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const ENDPOINT_TYPES = Object.freeze({
  UNKNOWN: 0,
  BROKER: 1,
  CONTROLLER: 2,
});

export type EndpointType = (typeof ENDPOINT_TYPES)[keyof typeof ENDPOINT_TYPES];
