import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { metadataRequestTopicEntries } from '../shared';
import type { MetadataRequestV11Fields } from '../v11/request';
import { requestSchema } from '../v11/request';

export type MetadataRequestV13Fields = MetadataRequestV11Fields;

/**
 * Metadata Request (Version: 13) — wire format identical to v11/v12. Response v13 adds a
 * top-level error code.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const create = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 13,
  apiName: 'Metadata',
  schema: requestSchema,
});

export const metadataRequestV13 = ({
  topics,
  topicIds,
  allowAutoTopicCreation,
  includeTopicAuthorizedOperations = false,
}: MetadataRequestV13Fields) =>
  create({
    topics: metadataRequestTopicEntries(topics, topicIds),
    allowAutoTopicCreation,
    includeTopicAuthorizedOperations,
  });
