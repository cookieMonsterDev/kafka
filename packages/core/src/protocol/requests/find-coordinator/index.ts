import type { ProtocolFactory, RequestFamily } from '../index';
import { findCoordinatorRequestV0 } from './v0/request';
import { findCoordinatorResponseV0 } from './v0/response';
import { findCoordinatorRequestV1 } from './v1/request';
import { findCoordinatorResponseV1 } from './v1/response';
import { findCoordinatorRequestV2 } from './v2/request';
import { findCoordinatorResponseV2 } from './v2/response';

export interface FindCoordinatorOptions {
  groupId?: string;
  coordinatorKey?: string;
  coordinatorType?: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<FindCoordinatorOptions>>> = {
  0: (options) => {
    const groupId = options.groupId ?? options.coordinatorKey;
    if (groupId == null) throw new Error('Invariant violated: FindCoordinator v0 requires groupId');
    return { request: findCoordinatorRequestV0({ groupId }), response: findCoordinatorResponseV0 };
  },
  1: (options) => {
    if (options.coordinatorKey == null || options.coordinatorType == null) {
      throw new Error('Invariant violated: FindCoordinator v1 requires coordinatorKey and coordinatorType');
    }
    return {
      request: findCoordinatorRequestV1({
        coordinatorKey: options.coordinatorKey,
        coordinatorType: options.coordinatorType,
      }),
      response: findCoordinatorResponseV1,
    };
  },
  2: (options) => {
    if (options.coordinatorKey == null || options.coordinatorType == null) {
      throw new Error('Invariant violated: FindCoordinator v2 requires coordinatorKey and coordinatorType');
    }
    return {
      request: findCoordinatorRequestV2({
        coordinatorKey: options.coordinatorKey,
        coordinatorType: options.coordinatorType,
      }),
      response: findCoordinatorResponseV2,
    };
  },
};

export const FindCoordinator: RequestFamily<FindCoordinatorOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no FindCoordinator protocol for version ${version}`);
    return factory;
  },
});
