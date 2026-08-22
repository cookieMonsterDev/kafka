import { compactNullableString, defineRequest, field, flexibleObject, int32, uuid } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface RemoveRaftVoterRequestV0Fields {
  clusterId: string | null;
  voterId: number;
  voterDirectoryId: Buffer;
}

/**
 * RemoveRaftVoter Request (Version: 0) => cluster_id voter_id voter_directory_id TAG_BUFFER
 *   cluster_id => COMPACT_NULLABLE_STRING
 *   voter_id => INT32
 *   voter_directory_id => UUID
 *
 * Flexible from v0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('clusterId', compactNullableString),
  field('voterId', int32),
  field('voterDirectoryId', uuid),
]);

export const removeRaftVoterRequestV0 = defineRequest({
  apiKey: API_KEYS.RemoveRaftVoter,
  apiVersion: 0,
  apiName: 'RemoveRaftVoter',
  schema: requestSchema,
});
