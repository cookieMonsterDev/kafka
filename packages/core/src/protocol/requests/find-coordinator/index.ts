import { COORDINATOR_TYPES } from '../../enums/coordinator-types';
import { defineRequestFamily, type VersionFactories, type VersionResponse } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { findCoordinatorRequestV0 } from './v0/request';
import { findCoordinatorResponseV0 } from './v0/response';
import { findCoordinatorRequestV1 } from './v1/request';
import { findCoordinatorResponseV1 } from './v1/response';
import { findCoordinatorRequestV2 } from './v2/request';
import { findCoordinatorResponseV2 } from './v2/response';
import { findCoordinatorRequestV3 } from './v3/request';
import { findCoordinatorResponseV3 } from './v3/response';

export interface FindCoordinatorOptions {
  groupId?: string;
  coordinatorKey?: string;
  coordinatorType?: number;
}

const VERSIONS = {
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
  3: (options) => {
    if (options.coordinatorKey == null || options.coordinatorType == null) {
      throw new Error('Invariant violated: FindCoordinator v3 requires coordinatorKey and coordinatorType');
    }
    return {
      request: findCoordinatorRequestV3({
        coordinatorKey: options.coordinatorKey,
        coordinatorType: options.coordinatorType,
      }),
      response: findCoordinatorResponseV3,
    };
  },
} satisfies VersionFactories<FindCoordinatorOptions>;

/** A FindCoordinator response, for every version. */
export type FindCoordinatorResponse = VersionResponse<typeof VERSIONS>;

export const FindCoordinator = defineRequestFamily(KAFKA_APIS.GroupCoordinator, VERSIONS, {
  // A group coordinator by default, keyed by the group id.
  prepareOptions: (options) => ({
    ...options,
    coordinatorKey: options.coordinatorKey ?? options.groupId,
    coordinatorType: options.coordinatorType ?? COORDINATOR_TYPES.GROUP,
  }),
});
