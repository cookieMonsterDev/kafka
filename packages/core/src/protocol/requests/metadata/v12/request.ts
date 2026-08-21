import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { metadataRequestTopicEntries } from '../shared';
import type { MetadataRequestV11Fields } from '../v11/request';
import { requestSchema } from '../v11/request';

export type MetadataRequestV12Fields = MetadataRequestV11Fields;

/**
 * Metadata Request (Version: 12) — wire format identical to v11. Response v12 makes topic
 * `name` nullable so a lookup by id can return a missing topic. This is the first version
 * where the broker uses topicId for real (KIP-516).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const create = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 12,
  apiName: 'Metadata',
  schema: requestSchema,
});

export const metadataRequestV12 = ({
  topics,
  topicIds,
  allowAutoTopicCreation,
  includeTopicAuthorizedOperations = false,
}: MetadataRequestV12Fields) =>
  create({
    topics: metadataRequestTopicEntries(topics, topicIds),
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
  });
