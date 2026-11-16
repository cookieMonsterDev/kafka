import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { type AlterReplicaLogDir, alterReplicaLogDirsRequestV0 } from './v0/request';
import { alterReplicaLogDirsResponseV0 } from './v0/response';
import { alterReplicaLogDirsRequestV1 } from './v1/request';
import { alterReplicaLogDirsResponseV1 } from './v1/response';
import { alterReplicaLogDirsRequestV2 } from './v2/request';
import { alterReplicaLogDirsResponseV2 } from './v2/response';

export interface AlterReplicaLogDirsOptions {
  dirs: AlterReplicaLogDir[];
}

const VERSIONS = {
  0: (options) => ({
    request: alterReplicaLogDirsRequestV0({ dirs: options.dirs }),
    response: alterReplicaLogDirsResponseV0,
  }),
  1: (options) => ({
    request: alterReplicaLogDirsRequestV1({ dirs: options.dirs }),
    response: alterReplicaLogDirsResponseV1,
  }),
  2: (options) => ({
    request: alterReplicaLogDirsRequestV2({ dirs: options.dirs }),
    response: alterReplicaLogDirsResponseV2,
  }),
} satisfies VersionFactories<AlterReplicaLogDirsOptions>;

export const AlterReplicaLogDirs = defineRequestFamily(KAFKA_APIS.AlterReplicaLogDirs, VERSIONS);
