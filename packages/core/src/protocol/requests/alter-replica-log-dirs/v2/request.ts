import { compactArray, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import type { AlterReplicaLogDir } from '../v0/request';

export type { AlterReplicaLogDir, AlterReplicaLogDirTopic } from '../v0/request';

export interface AlterReplicaLogDirsRequestV2Fields {
  dirs: AlterReplicaLogDir[];
}

/**
 * AlterReplicaLogDirs Request (Version: 2) => [dirs] TAG_BUFFER
 *   dirs => path [topics] TAG_BUFFER
 *     path => COMPACT_STRING
 *     topics => topic [partitions] TAG_BUFFER
 *       topic => COMPACT_STRING
 *       partitions => INT32
 *
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
const dirSchema = flexibleObject([field('path', compactString), field('topics', compactArray(topicSchema))]);
export const requestSchema = flexibleObject([field('dirs', compactArray(dirSchema))]);

export const alterReplicaLogDirsRequestV2 = defineRequest({
  apiKey: API_KEYS.AlterReplicaLogDirs,
  apiVersion: 2,
  apiName: 'AlterReplicaLogDirs',
  schema: requestSchema,
});
