import {
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
  uuid,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface AddRaftVoterListener {
  name: string;
  host: string;
  port: number;
}

export interface AddRaftVoterRequestV0Fields {
  clusterId: string | null;
  timeoutMs: number;
  voterId: number;
  voterDirectoryId: Buffer;
  listeners: AddRaftVoterListener[];
}

/**
 * AddRaftVoter Request (Version: 0) => cluster_id timeout_ms voter_id voter_directory_id
 *                                            [listeners] TAG_BUFFER
 *   cluster_id => COMPACT_NULLABLE_STRING
 *   timeout_ms => INT32
 *   voter_id => INT32
 *   voter_directory_id => UUID
 *   listeners => name host port TAG_BUFFER
 *     name => COMPACT_STRING
 *     host => COMPACT_STRING
 *     port => INT16
 *
 * Flexible from v0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const listenerSchema = flexibleObject([
  field('name', compactString),
  field('host', compactString),
  field('port', int16),
]);
export const requestSchema = flexibleObject([
  field('clusterId', compactNullableString),
  field('timeoutMs', int32),
  field('voterId', int32),
  field('voterDirectoryId', uuid),
  field('listeners', compactArray(listenerSchema)),
]);

export const addRaftVoterRequestV0 = defineRequest({
  apiKey: API_KEYS.AddRaftVoter,
  apiVersion: 0,
  apiName: 'AddRaftVoter',
  schema: requestSchema,
});
