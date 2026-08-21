import { compactArray, compactString, defineRequest, field, flexibleObject, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { CreateDelegationTokenRequestV0Fields } from '../v0/request';

export type CreateDelegationTokenRequestV2Fields = CreateDelegationTokenRequestV0Fields;

/**
 * CreateDelegationToken Request (Version: 2) => [renewers] max_lifetime_ms TAG_BUFFER
 *   renewers => principal_type principal_name TAG_BUFFER
 *
 * Flexible compact + tagged form of v1 (KIP-482). Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const renewerSchema = flexibleObject([field('principalType', compactString), field('name', compactString)]);
export const requestSchema = flexibleObject([
  field('renewers', compactArray(renewerSchema)),
  field('maxLifetimeMs', int64),
]);

export const createDelegationTokenRequestV2 = defineRequest({
  apiKey: API_KEYS.CreateDelegationToken,
  apiVersion: 2,
  apiName: 'CreateDelegationToken',
  schema: requestSchema,
});
