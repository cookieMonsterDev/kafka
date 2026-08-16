import type { ProtocolFactory, RequestFamily } from '../index';
import { type AlterReplicaLogDir, alterReplicaLogDirsRequestV0 } from './v0/request';
import { alterReplicaLogDirsResponseV0 } from './v0/response';
import { alterReplicaLogDirsRequestV1 } from './v1/request';
import { alterReplicaLogDirsResponseV1 } from './v1/response';
import { alterReplicaLogDirsRequestV2 } from './v2/request';
import { alterReplicaLogDirsResponseV2 } from './v2/response';

export interface AlterReplicaLogDirsOptions {
  dirs: AlterReplicaLogDir[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<AlterReplicaLogDirsOptions>>> = {
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
};

export const AlterReplicaLogDirs: RequestFamily<AlterReplicaLogDirsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AlterReplicaLogDirs protocol for version ${version}`);
    return factory;
  },
});
