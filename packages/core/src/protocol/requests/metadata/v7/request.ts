import { boolean, defineRequest, field, nullableArray, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * Metadata Request (Version: 7) => [topics] allow_auto_topic_creation
 *   topics => STRING
 *   allow_auto_topic_creation => BOOLEAN
 *
 * Wire format is identical to v6. The bump is on the response (KIP-320 `leader_epoch`).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const requestSchema = object([field('topics', nullableArray(string)), field('allowAutoTopicCreation', boolean)]);

export const metadataRequestV7 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 7,
  apiName: 'Metadata',
  schema: requestSchema,
});
