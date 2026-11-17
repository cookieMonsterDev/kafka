import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import {
  type AlterClientQuotasEntry,
  type AlterClientQuotasEntity,
  type AlterClientQuotasOp,
  alterClientQuotasRequestV0,
} from './v0/request';
import { alterClientQuotasResponseV0 } from './v0/response';
import { alterClientQuotasRequestV1 } from './v1/request';
import { alterClientQuotasResponseV1 } from './v1/response';

export type { AlterClientQuotasEntry, AlterClientQuotasEntity, AlterClientQuotasOp };

export interface AlterClientQuotasOptions {
  entries: AlterClientQuotasEntry[];
  validateOnly?: boolean;
}

const VERSIONS = {
  0: (options) => ({
    request: alterClientQuotasRequestV0({
      entries: options.entries,
      validateOnly: options.validateOnly ?? false,
    }),
    response: alterClientQuotasResponseV0,
  }),
  1: (options) => ({
    request: alterClientQuotasRequestV1({
      entries: options.entries,
      validateOnly: options.validateOnly ?? false,
    }),
    response: alterClientQuotasResponseV1,
  }),
} satisfies VersionFactories<AlterClientQuotasOptions>;

export const AlterClientQuotas = defineRequestFamily(KAFKA_APIS.AlterClientQuotas, VERSIONS);
