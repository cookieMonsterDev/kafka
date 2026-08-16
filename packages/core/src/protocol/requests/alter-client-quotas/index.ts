import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<AlterClientQuotasOptions>>> = {
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
};

export const AlterClientQuotas: RequestFamily<AlterClientQuotasOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AlterClientQuotas protocol for version ${version}`);
    return factory;
  },
});
