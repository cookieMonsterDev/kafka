import { boolean, defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface MetadataRequestV8Fields {
  topics: string[];
  allowAutoTopicCreation: boolean;
  includeClusterAuthorizedOperations?: boolean;
  includeTopicAuthorizedOperations?: boolean;
}

/**
 * Metadata Request (Version: 8) => [topics] allow_auto_topic_creation
 *                                  include_cluster_authorized_operations
 *                                  include_topic_authorized_operations
 *   topics => STRING
 *   allow_auto_topic_creation => BOOLEAN
 *   include_cluster_authorized_operations => BOOLEAN
 *   include_topic_authorized_operations => BOOLEAN
 *
 * Adds the authorized-operations include flags (KIP-430). Defaults are false so ordinary
 * producer/consumer metadata fetches skip the expensive ACL bitmask.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const requestSchema = object([
  field('topics', nullableArray(string)),
  field('allowAutoTopicCreation', boolean),
  field('includeClusterAuthorizedOperations', boolean),
  field('includeTopicAuthorizedOperations', boolean),
]);

const create = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 8,
  apiName: 'Metadata',
  schema: requestSchema,
});

export const metadataRequestV8 = ({
  topics,
  allowAutoTopicCreation,
  includeClusterAuthorizedOperations = false,
  includeTopicAuthorizedOperations = false,
}: MetadataRequestV8Fields) =>
  create({
    topics,
    allowAutoTopicCreation,
    includeClusterAuthorizedOperations,
    includeTopicAuthorizedOperations,
  });
