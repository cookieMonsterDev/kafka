import {
  boolean,
  compactArray,
  compactNullableArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int8,
} from '../../../schema';
import type { FieldCodec } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export {
  type DescribeConfigsRequestV3Fields as DescribeConfigsRequestV4Fields,
  type DescribeConfigsResource,
  withDefaultConfigNames,
} from '../v3/request';

/**
 * Compact array that writes `[]` as wire-null (uvarint 0), matching non-flexible
 * `nullableArray`: empty `configNames` means "return every key".
 */
const compactConfigNames: FieldCodec<string[]> = {
  write(encoder, values) {
    compactNullableArray(compactString).write(encoder, values.length === 0 ? null : values);
  },
  read(decoder) {
    return compactNullableArray(compactString).read(decoder) ?? [];
  },
};

/**
 * DescribeConfigs Request (Version: 4) => [resources] include_synonyms include_documentation TAG_BUFFER
 *   resources => resource_type resource_name [config_names] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     config_names => COMPACT_STRING
 *   include_synonyms => BOOLEAN
 *   include_documentation => BOOLEAN
 *
 * Flexible compact + tagged form of v3 (KIP-482). Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 */
const resourceSchema = flexibleObject([
  field('type', int8),
  field('name', compactString),
  field('configNames', compactConfigNames),
]);
export const requestSchema = flexibleObject([
  field('resources', compactArray(resourceSchema)),
  field('includeSynonyms', boolean),
  field('includeDocumentation', boolean),
]);

export const describeConfigsRequestV4 = defineRequest({
  apiKey: API_KEYS.DescribeConfigs,
  apiVersion: 4,
  apiName: 'DescribeConfigs',
  schema: requestSchema,
});
