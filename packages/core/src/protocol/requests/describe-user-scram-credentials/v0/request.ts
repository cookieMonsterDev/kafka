import { compactNullableArray, compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeUserScramCredentialsRequestV0Fields {
  users: { name: string }[] | null;
}

/**
 * DescribeUserScramCredentials Request (Version: 0) => [users] TAG_BUFFER
 *   users => name TAG_BUFFER
 *     name => COMPACT_STRING
 *
 * Flexible from v0. `users: null` (or empty) describes every user.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const userSchema = flexibleObject([field('name', compactString)]);
export const requestSchema = flexibleObject([field('users', compactNullableArray(userSchema))]);

export const describeUserScramCredentialsRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeUserScramCredentials,
  apiVersion: 0,
  apiName: 'DescribeUserScramCredentials',
  schema: requestSchema,
});
