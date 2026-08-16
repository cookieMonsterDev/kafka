import {
  compactArray,
  compactBytes,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int32,
  int8,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ScramCredentialDeletion {
  name: string;
  mechanism: number;
}

export interface ScramCredentialUpsertion {
  name: string;
  mechanism: number;
  iterations: number;
  salt: Buffer;
  saltedPassword: Buffer;
}

export interface AlterUserScramCredentialsRequestV0Fields {
  deletions: ScramCredentialDeletion[];
  upsertions: ScramCredentialUpsertion[];
}

/**
 * AlterUserScramCredentials Request (Version: 0) => [deletions] [upsertions] TAG_BUFFER
 *
 * Flexible from v0. `saltedPassword` is Hi(password, salt, iterations) (RFC 5802).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const deletionSchema = flexibleObject([field('name', compactString), field('mechanism', int8)]);
const upsertionSchema = flexibleObject([
  field('name', compactString),
  field('mechanism', int8),
  field('iterations', int32),
  field('salt', compactBytes),
  field('saltedPassword', compactBytes),
]);
export const requestSchema = flexibleObject([
  field('deletions', compactArray(deletionSchema)),
  field('upsertions', compactArray(upsertionSchema)),
]);

export const alterUserScramCredentialsRequestV0 = defineRequest({
  apiKey: API_KEYS.AlterUserScramCredentials,
  apiVersion: 0,
  apiName: 'AlterUserScramCredentials',
  schema: requestSchema,
});
