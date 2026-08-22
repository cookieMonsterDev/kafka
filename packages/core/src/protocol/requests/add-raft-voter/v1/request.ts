import {
  boolean,
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
import type { AddRaftVoterListener } from '../v0/request';

export interface AddRaftVoterRequestV1Fields {
  clusterId: string | null;
  timeoutMs: number;
  voterId: number;
  voterDirectoryId: Buffer;
  listeners: AddRaftVoterListener[];
  ackWhenCommitted: boolean;
}

/**
 * AddRaftVoter Request (Version: 1) => cluster_id timeout_ms voter_id voter_directory_id
 *                                            [listeners] ack_when_committed TAG_BUFFER
 *   ack_when_committed => BOOLEAN
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
  field('ackWhenCommitted', boolean),
]);

export const addRaftVoterRequestV1 = defineRequest({
  apiKey: API_KEYS.AddRaftVoter,
  apiVersion: 1,
  apiName: 'AddRaftVoter',
  schema: requestSchema,
});
