import type { ProtocolFactory, RequestFamily } from '../index';
import type { DeleteRecordsTopic } from './v0/request';
import { deleteRecordsRequestV0 } from './v0/request';
import { deleteRecordsResponseV0 } from './v0/response';
import { deleteRecordsRequestV1 } from './v1/request';
import { deleteRecordsResponseV1 } from './v1/response';

export interface DeleteRecordsOptions {
  topics: DeleteRecordsTopic[];
  timeout?: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DeleteRecordsOptions>>> = {
  0: (options) => ({
    request: deleteRecordsRequestV0({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteRecordsResponseV0({ topics: options.topics }),
  }),
  1: (options) => ({
    request: deleteRecordsRequestV1({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteRecordsResponseV1({ topics: options.topics }),
  }),
};

export const DeleteRecords: RequestFamily<DeleteRecordsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteRecords protocol for version ${version}`);
    return factory;
  },
});
