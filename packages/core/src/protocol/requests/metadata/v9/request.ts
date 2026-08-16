import { boolean, compactNullableArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface MetadataRequestV9Fields {
  topics: string[];
  allowAutoTopicCreation: boolean;
  includeClusterAuthorizedOperations?: boolean;
  includeTopicAuthorizedOperations?: boolean;
}

/**
 * Metadata Request (Version: 9) => [topics] allow_auto_topic_creation
 *                                  include_cluster_authorized_operations
 *                                  include_topic_authorized_operations TAG_BUFFER
 *   topics => name TAG_BUFFER
 *     name => COMPACT_STRING
 *   allow_auto_topic_creation => BOOLEAN
 *   include_cluster_authorized_operations => BOOLEAN
 *   include_topic_authorized_operations => BOOLEAN
 *
 * First flexible Metadata version (KIP-482). Empty `topics` is written as a compact null
 * ("all topics"), matching `nullableArray` on v1–v8. Request header v2's trailing TAG_BUFFER
 * is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('name', compactString)]);
const requestSchema = flexibleObject([
  field('topics', compactNullableArray(topicSchema)),
  field('allowAutoTopicCreation', boolean),
  field('includeClusterAuthorizedOperations', boolean),
  field('includeTopicAuthorizedOperations', boolean),
]);

const create = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 9,
  apiName: 'Metadata',
  schema: requestSchema,
});

export const metadataRequestV9 = ({
  topics,
  allowAutoTopicCreation,
  includeClusterAuthorizedOperations = false,
  includeTopicAuthorizedOperations = false,
}: MetadataRequestV9Fields) =>
  create({
    topics: topics.length === 0 ? null : topics.map((name) => ({ name })),
    allowAutoTopicCreation,
    includeClusterAuthorizedOperations,
    includeTopicAuthorizedOperations,
  });
