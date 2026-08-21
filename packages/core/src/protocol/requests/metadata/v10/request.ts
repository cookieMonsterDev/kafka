import {
  boolean,
  compactNullableArray,
  compactNullableString,
  defineRequest,
  field,
  flexibleObject,
  uuid,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { metadataRequestTopicEntries } from '../shared';

export interface MetadataRequestV10Fields {
  topics: string[];
  topicIds?: Buffer[];
  allowAutoTopicCreation: boolean;
  includeClusterAuthorizedOperations?: boolean;
  includeTopicAuthorizedOperations?: boolean;
}

/**
 * Metadata Request (Version: 10) => [topics] allow_auto_topic_creation
 *                                   include_cluster_authorized_operations
 *                                   include_topic_authorized_operations TAG_BUFFER
 *   topics => topic_id name TAG_BUFFER
 *     topic_id => UUID
 *     name => COMPACT_NULLABLE_STRING
 *   allow_auto_topic_creation => BOOLEAN
 *   include_cluster_authorized_operations => BOOLEAN
 *   include_topic_authorized_operations => BOOLEAN
 *
 * Adds `topicId` and a nullable name on each topic (KIP-516). Apache: versions 10 and 11
 * should not use topicId or null names on the server — this encoder still writes the fields
 * (zero UUID + name) so the wire is correct. Empty `topics` is a compact null ("all topics").
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const topicSchema = flexibleObject([field('topicId', uuid), field('name', compactNullableString)]);
export const requestSchema = flexibleObject([
  field('topics', compactNullableArray(topicSchema)),
  field('allowAutoTopicCreation', boolean),
  field('includeClusterAuthorizedOperations', boolean),
  field('includeTopicAuthorizedOperations', boolean),
]);

const create = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 10,
  apiName: 'Metadata',
  schema: requestSchema,
});

export const metadataRequestV10 = ({
  topics,
  topicIds,
  allowAutoTopicCreation,
  includeClusterAuthorizedOperations = false,
  includeTopicAuthorizedOperations = false,
}: MetadataRequestV10Fields) =>
  create({
    topics: metadataRequestTopicEntries(topics, topicIds),
    allowAutoTopicCreation,
    includeClusterAuthorizedOperations,
    includeTopicAuthorizedOperations,
  });
