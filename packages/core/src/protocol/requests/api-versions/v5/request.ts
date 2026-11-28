import { compactNullableString, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import {
  DEFAULT_CLIENT_SOFTWARE_NAME,
  DEFAULT_CLIENT_SOFTWARE_VERSION,
  type ApiVersionsRequestOptions,
} from '../v3/request';

/** The node id that tells the broker the client does not know which node it reached. */
const UNKNOWN_NODE_ID = -1;

/**
 * ApiVersions Request (Version: 5) => client_software_name client_software_version cluster_id node_id TAG_BUFFER
 *   client_software_name => COMPACT_STRING
 *   client_software_version => COMPACT_STRING
 *   cluster_id => COMPACT_NULLABLE_STRING
 *   node_id => INT32
 *
 * Adds the cluster id and node id the client expects (KIP-1242). A broker whose own ids differ
 * answers `REBOOTSTRAP_REQUIRED`. A bootstrap connection sends `null` and `-1`, which the broker
 * does not check. Request header is v2; response header stays v0.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const requestSchema = flexibleObject([
  field('clientSoftwareName', compactString),
  field('clientSoftwareVersion', compactString),
  field('clusterId', compactNullableString),
  field('nodeId', int32),
]);

const create = defineRequest({
  apiKey: API_KEYS.ApiVersions,
  apiVersion: 5,
  apiName: 'ApiVersions',
  schema: requestSchema,
});

export const apiVersionsRequestV5 = (options: ApiVersionsRequestOptions = {}) =>
  create({
    clientSoftwareName: options.clientSoftwareName ?? DEFAULT_CLIENT_SOFTWARE_NAME,
    clientSoftwareVersion: options.clientSoftwareVersion ?? DEFAULT_CLIENT_SOFTWARE_VERSION,
    clusterId: options.clusterId ?? null,
    nodeId: options.nodeId ?? UNKNOWN_NODE_ID,
  });
