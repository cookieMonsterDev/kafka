import { compactString, defineRequest, field, flexibleObject } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ApiVersionsRequestOptions {
  clientSoftwareName?: string;
  clientSoftwareVersion?: string;
}

/** Matches `@kafka/core`'s package version; brokers only log this (KIP-511). */
export const DEFAULT_CLIENT_SOFTWARE_NAME = 'kafka';
export const DEFAULT_CLIENT_SOFTWARE_VERSION = '0.0.0';

/**
 * ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
 *   client_software_name => COMPACT_STRING
 *   client_software_version => COMPACT_STRING
 *
 * First flexible ApiVersions body (KIP-511). The request header is v2 (TAG_BUFFER after
 * client id); the response header stays v0. See `usesFlexibleRequestHeader`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const requestSchema = flexibleObject([
  field('clientSoftwareName', compactString),
  field('clientSoftwareVersion', compactString),
]);

const create = defineRequest({
  apiKey: API_KEYS.ApiVersions,
  apiVersion: 3,
  apiName: 'ApiVersions',
  schema: requestSchema,
});

export const apiVersionsRequestV3 = (options: ApiVersionsRequestOptions = {}) =>
  create({
    clientSoftwareName: options.clientSoftwareName ?? DEFAULT_CLIENT_SOFTWARE_NAME,
    clientSoftwareVersion: options.clientSoftwareVersion ?? DEFAULT_CLIENT_SOFTWARE_VERSION,
  });
