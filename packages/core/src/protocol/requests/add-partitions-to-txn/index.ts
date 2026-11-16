import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { addPartitionsToTxnRequestV0 } from './v0/request';
import { addPartitionsToTxnResponseV0 } from './v0/response';
import { addPartitionsToTxnRequestV1 } from './v1/request';
import { addPartitionsToTxnResponseV1 } from './v1/response';
import { addPartitionsToTxnRequestV2 } from './v2/request';
import { addPartitionsToTxnResponseV2 } from './v2/response';
import { addPartitionsToTxnRequestV3 } from './v3/request';
import { addPartitionsToTxnResponseV3 } from './v3/response';

export interface AddPartitionsToTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  topics: { topic: string; partitions: number[] }[];
}

const VERSIONS = {
  0: (values) => ({ request: addPartitionsToTxnRequestV0(values), response: addPartitionsToTxnResponseV0 }),
  1: (values) => ({ request: addPartitionsToTxnRequestV1(values), response: addPartitionsToTxnResponseV1 }),
  2: (values) => ({ request: addPartitionsToTxnRequestV2(values), response: addPartitionsToTxnResponseV2 }),
  3: (values) => ({ request: addPartitionsToTxnRequestV3(values), response: addPartitionsToTxnResponseV3 }),
} satisfies VersionFactories<AddPartitionsToTxnOptions>;

export const AddPartitionsToTxn = defineRequestFamily(KAFKA_APIS.AddPartitionsToTxn, VERSIONS);
