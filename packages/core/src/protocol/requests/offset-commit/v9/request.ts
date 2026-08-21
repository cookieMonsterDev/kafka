import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v8/request';

/**
 * OffsetCommit Request (Version: 9) is the same as version 8. Version 9 is the first that can
 * be used with the new consumer group protocol (KIP-848); `groupGenerationId` carries the
 * member epoch.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export { requestSchema };

export const offsetCommitRequestV9 = defineRequest({
  apiKey: API_KEYS.OffsetCommit,
  apiVersion: 9,
  apiName: 'OffsetCommit',
  schema: requestSchema,
});
