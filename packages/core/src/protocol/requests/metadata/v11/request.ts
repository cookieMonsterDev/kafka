import { boolean, compactNullableArray, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { metadataRequestTopicEntries } from '../shared';
import { topicSchema } from '../v10/request';

export interface MetadataRequestV11Fields {
  topics: string[];
  topicIds?: Buffer[];
  allowAutoTopicCreation: boolean;
  includeTopicAuthorizedOperations?: boolean;
}

/**
 * Metadata Request (Version: 11) => [topics] allow_auto_topic_creation
 *                                   include_topic_authorized_operations TAG_BUFFER
 *   topics => topic_id name TAG_BUFFER
 *     topic_id => UUID
 *     name => COMPACT_NULLABLE_STRING
 *   allow_auto_topic_creation => BOOLEAN
 *   include_topic_authorized_operations => BOOLEAN
 *
 * Drops `includeClusterAuthorizedOperations` (KIP-700; now on DescribeCluster). Topic structs
 * still carry topicId + nullable name; Apache: v10–v11 should not use those on the server.
 * v12 and v13 reuse this request body.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('topics', compactNullableArray(topicSchema)),
  field('allowAutoTopicCreation', boolean),
  field('includeTopicAuthorizedOperations', boolean),
]);

const create = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 11,
  apiName: 'Metadata',
  schema: requestSchema,
});

export const metadataRequestV11 = ({
  topics,
  topicIds,
  allowAutoTopicCreation,
  includeTopicAuthorizedOperations = false,
}: MetadataRequestV11Fields) =>
  create({
    topics: metadataRequestTopicEntries(topics, topicIds),
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
  });
