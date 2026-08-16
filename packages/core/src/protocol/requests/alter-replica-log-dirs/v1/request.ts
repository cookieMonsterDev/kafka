import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { requestSchema } from '../v0/request';

export {
  type AlterReplicaLogDir,
  type AlterReplicaLogDirTopic,
  type AlterReplicaLogDirsRequestV0Fields as AlterReplicaLogDirsRequestV1Fields,
} from '../v0/request';

/**
 * AlterReplicaLogDirs Request (Version: 1) — wire format identical to v0; only the response's
 * throttling semantics change (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const alterReplicaLogDirsRequestV1 = defineRequest({
  apiKey: API_KEYS.AlterReplicaLogDirs,
  apiVersion: 1,
  apiName: 'AlterReplicaLogDirs',
  schema: requestSchema,
});
