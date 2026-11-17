import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { addOffsetsToTxnRequestV0 } from './v0/request';
import { addOffsetsToTxnResponseV0 } from './v0/response';
import { addOffsetsToTxnRequestV1 } from './v1/request';
import { addOffsetsToTxnResponseV1 } from './v1/response';
import { addOffsetsToTxnRequestV2 } from './v2/request';
import { addOffsetsToTxnResponseV2 } from './v2/response';
import { addOffsetsToTxnRequestV3 } from './v3/request';
import { addOffsetsToTxnResponseV3 } from './v3/response';

export interface AddOffsetsToTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  groupId: string;
}

const VERSIONS = {
  0: (values) => ({ request: addOffsetsToTxnRequestV0(values), response: addOffsetsToTxnResponseV0 }),
  1: (values) => ({ request: addOffsetsToTxnRequestV1(values), response: addOffsetsToTxnResponseV1 }),
  2: (values) => ({ request: addOffsetsToTxnRequestV2(values), response: addOffsetsToTxnResponseV2 }),
  3: (values) => ({ request: addOffsetsToTxnRequestV3(values), response: addOffsetsToTxnResponseV3 }),
} satisfies VersionFactories<AddOffsetsToTxnOptions>;

export const AddOffsetsToTxn = defineRequestFamily(KAFKA_APIS.AddOffsetsToTxn, VERSIONS);
