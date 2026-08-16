import { array, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface AlterReplicaLogDirTopic {
  topic: string;
  partitions: number[];
}

export interface AlterReplicaLogDir {
  path: string;
  topics: AlterReplicaLogDirTopic[];
}

export interface AlterReplicaLogDirsRequestV0Fields {
  dirs: AlterReplicaLogDir[];
}

/**
 * AlterReplicaLogDirs Request (Version: 0) => [dirs]
 *   dirs => path [topics]
 *     path => STRING
 *     topics => topic [partitions]
 *       topic => STRING
 *       partitions => INT32
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
const dirSchema = object([field('path', string), field('topics', array(topicSchema))]);
export const requestSchema = object([field('dirs', array(dirSchema))]);

export const alterReplicaLogDirsRequestV0 = defineRequest({
  apiKey: API_KEYS.AlterReplicaLogDirs,
  apiVersion: 0,
  apiName: 'AlterReplicaLogDirs',
  schema: requestSchema,
});
