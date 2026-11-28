/**
 * Whether a StreamsGroupDescribe v1 response carries a group's topology description (KIP-1331).
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/StreamsGroupDescribeResponse.json
 */
export const STREAMS_TOPOLOGY_DESCRIPTION_STATUSES = Object.freeze({
  /** The request did not ask for it. */
  NOT_REQUESTED: 0,
  /** The broker has no description recorded for the group. */
  NOT_STORED: 1,
  /** The broker failed to fetch the description; its logs say why. */
  ERROR: 2,
  /** `topologyDescription` holds the description. */
  AVAILABLE: 3,
});

export type StreamsTopologyDescriptionStatus =
  (typeof STREAMS_TOPOLOGY_DESCRIPTION_STATUSES)[keyof typeof STREAMS_TOPOLOGY_DESCRIPTION_STATUSES];
