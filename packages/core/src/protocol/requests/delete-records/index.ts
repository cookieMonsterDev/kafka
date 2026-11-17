import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import type { DeleteRecordsTopic } from './v0/request';
import { deleteRecordsRequestV0 } from './v0/request';
import { deleteRecordsResponseV0 } from './v0/response';
import { deleteRecordsRequestV1 } from './v1/request';
import { deleteRecordsResponseV1 } from './v1/response';
import { deleteRecordsRequestV2 } from './v2/request';
import { deleteRecordsResponseV2 } from './v2/response';

export interface DeleteRecordsOptions {
  topics: DeleteRecordsTopic[];
  timeout?: number;
}

const VERSIONS = {
  0: (options) => ({
    request: deleteRecordsRequestV0({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteRecordsResponseV0({ topics: options.topics }),
  }),
  1: (options) => ({
    request: deleteRecordsRequestV1({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteRecordsResponseV1({ topics: options.topics }),
  }),
  2: (options) => ({
    request: deleteRecordsRequestV2({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteRecordsResponseV2({ topics: options.topics }),
  }),
} satisfies VersionFactories<DeleteRecordsOptions>;

export const DeleteRecords = defineRequestFamily(KAFKA_APIS.DeleteRecords, VERSIONS);
