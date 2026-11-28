import { KafkaNonRetriableError } from '../../../errors';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { streamsGroupDescribeRequestV0 } from './v0/request';
import { streamsGroupDescribeResponseV0 } from './v0/response';
import { streamsGroupDescribeRequestV1 } from './v1/request';
import { streamsGroupDescribeResponseV1 } from './v1/response';

export type { StreamsGroupDescribeResponseV0Body } from './v0/response';
export type { StreamsGroupDescribeResponseV1Body } from './v1/response';

export interface StreamsGroupDescribeOptions {
  groupIds: string[];
  includeAuthorizedOperations?: boolean;
  includeTopologyDescription?: boolean;
}

const fields = (options: StreamsGroupDescribeOptions) => ({
  groupIds: options.groupIds,
  includeAuthorizedOperations: options.includeAuthorizedOperations ?? false,
});

const VERSIONS = {
  0: (options) => {
    // Version 0 cannot ask for the topology description, so the request throws instead of
    // answering every group with `NOT_REQUESTED`.
    if (options.includeTopologyDescription === true) {
      throw new KafkaNonRetriableError(
        'StreamsGroupDescribe v0 cannot include topology descriptions; this broker needs StreamsGroupDescribe v1 (Kafka 4.4+)',
      );
    }
    return {
      request: streamsGroupDescribeRequestV0(fields(options)),
      response: streamsGroupDescribeResponseV0,
    };
  },
  1: (options) => ({
    request: streamsGroupDescribeRequestV1({
      ...fields(options),
      includeTopologyDescription: options.includeTopologyDescription ?? false,
    }),
    response: streamsGroupDescribeResponseV1,
  }),
} satisfies VersionFactories<StreamsGroupDescribeOptions>;

export const StreamsGroupDescribe = defineRequestFamily(KAFKA_APIS.StreamsGroupDescribe, VERSIONS);
