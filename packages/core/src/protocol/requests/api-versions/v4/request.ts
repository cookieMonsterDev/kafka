import { compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import {
  DEFAULT_CLIENT_SOFTWARE_NAME,
  DEFAULT_CLIENT_SOFTWARE_VERSION,
  type ApiVersionsRequestOptions,
} from '../v3/request';

export type { ApiVersionsRequestOptions };

/**
 * ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER
 *
 * Same compact body as v3. Version 4 exists so the matching response can advertise
 * SupportedFeatures.MinVersion 0 (KAFKA-17011). Request header is v2; response header stays v0.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const requestSchema = flexibleObject([
  field('clientSoftwareName', compactString),
  field('clientSoftwareVersion', compactString),
]);

const create = defineRequest({
  apiKey: API_KEYS.ApiVersions,
  apiVersion: 4,
  apiName: 'ApiVersions',
  schema: requestSchema,
});

export const apiVersionsRequestV4 = (options: ApiVersionsRequestOptions = {}) =>
  create({
    clientSoftwareName: options.clientSoftwareName ?? DEFAULT_CLIENT_SOFTWARE_NAME,
    clientSoftwareVersion: options.clientSoftwareVersion ?? DEFAULT_CLIENT_SOFTWARE_VERSION,
  });
