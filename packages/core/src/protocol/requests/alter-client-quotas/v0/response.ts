import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, int16, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface AlterClientQuotasEntityResult {
  entityType: string;
  entityName: string | null;
}

export interface AlterClientQuotasEntryResult {
  errorCode: number;
  errorMessage: string | null;
  entity: AlterClientQuotasEntityResult[];
}

export interface AlterClientQuotasResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  entries: AlterClientQuotasEntryResult[];
}

/**
 * AlterClientQuotas Response (Version: 0) => throttle_time_ms [entries]
 *   throttle_time_ms => INT32
 *   entries => error_code error_message [entity]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     entity => entity_type entity_name
 *       entity_type => STRING
 *       entity_name => NULLABLE_STRING
 *
 * KIP-546. The wire's throttle_time_ms is client-side (KIP-219); exposed as `clientSideThrottleTime`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const entitySchema = object([field('entityType', string), field('entityName', nullableString)]);
const entrySchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('entity', array(entitySchema)),
]);
const restSchema = object([field('entries', array(entrySchema))]);

export const alterClientQuotasResponseV0: ResponseDefinition<AlterClientQuotasResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime: 0, clientSideThrottleTime, ...rest };
  },
  parse: async (data) => {
    const entryWithError = data.entries.find((entry) => failure(entry.errorCode));
    if (entryWithError) throw createErrorFromCode(entryWithError.errorCode);
    return data;
  },
};
