import {
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { CreateDelegationTokenRenewer } from '../v0/request';

export interface CreateDelegationTokenRequestV3Fields {
  ownerPrincipalType: string | null;
  ownerPrincipalName: string | null;
  renewers: CreateDelegationTokenRenewer[];
  maxLifetimeMs: bigint;
}

/**
 * CreateDelegationToken Request (Version: 3) => owner_principal_type owner_principal_name
 *   [renewers] max_lifetime_ms TAG_BUFFER
 *
 * Version 3 adds a nullable owner principal. Null owner fields default to the request principal.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const renewerSchema = flexibleObject([field('principalType', compactString), field('name', compactString)]);
export const requestSchema = flexibleObject([
  field('ownerPrincipalType', compactNullableString),
  field('ownerPrincipalName', compactNullableString),
  field('renewers', compactArray(renewerSchema)),
  field('maxLifetimeMs', int64),
]);

export const createDelegationTokenRequestV3 = defineRequest({
  apiKey: API_KEYS.CreateDelegationToken,
  apiVersion: 3,
  apiName: 'CreateDelegationToken',
  schema: requestSchema,
});
