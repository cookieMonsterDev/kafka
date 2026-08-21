import { array, defineRequest, field, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface CreateDelegationTokenRenewer {
  principalType: string;
  name: string;
}

export interface CreateDelegationTokenRequestV0Fields {
  renewers: CreateDelegationTokenRenewer[];
  maxLifetimeMs: bigint;
}

/**
 * CreateDelegationToken Request (Version: 0) => [renewers] max_life_time
 *   renewers => principal_type principal_name
 *     principal_type => STRING
 *     principal_name => STRING
 *   max_life_time => INT64
 *
 * Version 1 is the same as version 0. Version 0 was removed from Kafka 4.0 brokers
 * (KIP-896); the client still encodes it for 1.1–3.x clusters.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const renewerSchema = object([field('principalType', string), field('name', string)]);
export const requestSchema = object([field('renewers', array(renewerSchema)), field('maxLifetimeMs', int64)]);

export const createDelegationTokenRequestV0 = defineRequest({
  apiKey: API_KEYS.CreateDelegationToken,
  apiVersion: 0,
  apiName: 'CreateDelegationToken',
  schema: requestSchema,
});
