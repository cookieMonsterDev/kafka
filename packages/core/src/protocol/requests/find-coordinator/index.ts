import type { CoordinatorType } from '../../enums/coordinator-types';
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
import { findCoordinatorRequestV4 } from './v4/request';
import { findCoordinatorResponseV4 } from './v4/response';

export interface FindCoordinatorOptions {
  /**
   * The keys to look up: group ids, or transactional ids with `COORDINATOR_TYPES.TRANSACTION`.
   * Versions before 4 take exactly one key; v4 (KIP-699) takes several of one type.
   */
  coordinatorKeys: readonly string[];
  /** Ignored by v0, which only finds group coordinators. */
  coordinatorType: CoordinatorType;
}

/** The broker that coordinates one key. */
export interface FoundCoordinator {
  key: string;
  nodeId: number;
  host: string;
  port: number;
}

/** A FindCoordinator response, for every version: one coordinator per requested key. */
export interface FindCoordinatorResponse {
  coordinators: FoundCoordinator[];
}

/** The one key a version before 4 looks up. */
function singleKey({ coordinatorKeys }: FindCoordinatorOptions): string {
  const [key] = coordinatorKeys;
  if (key === undefined || coordinatorKeys.length > 1) {
    throw new Error('Invariant violated: FindCoordinator before v4 looks up exactly one key');
  }
  return key;
}

const VERSIONS = {
  0: (options) => ({
    request: findCoordinatorRequestV0({ groupId: singleKey(options) }),
    response: findCoordinatorResponseV0,
  }),
  1: (options) => ({
    request: findCoordinatorRequestV1({
      coordinatorKey: singleKey(options),
      coordinatorType: options.coordinatorType,
    }),
    response: findCoordinatorResponseV1,
  }),
  2: (options) => ({
    request: findCoordinatorRequestV2({
      coordinatorKey: singleKey(options),
      coordinatorType: options.coordinatorType,
    }),
    response: findCoordinatorResponseV2,
  }),
  3: (options) => ({
    request: findCoordinatorRequestV3({
      coordinatorKey: singleKey(options),
      coordinatorType: options.coordinatorType,
    }),
    response: findCoordinatorResponseV3,
  }),
  4: (options) => ({
    request: findCoordinatorRequestV4({
      coordinatorKeys: [...options.coordinatorKeys],
      coordinatorType: options.coordinatorType,
    }),
    response: findCoordinatorResponseV4,
  }),
} satisfies VersionFactories<FindCoordinatorOptions>;

export const FindCoordinator = defineRequestFamily<typeof VERSIONS, FindCoordinatorResponse>(
  KAFKA_APIS.GroupCoordinator,
  VERSIONS,
  {
    // Versions before 4 answer for their one key without naming it.
    parseResponse: async (body, options, parse) => {
      const parsed = (await parse(body)) as VersionResponse<typeof VERSIONS>;
      if ('coordinators' in parsed) {
        return {
          coordinators: parsed.coordinators.map(({ key, nodeId, host, port }) => ({ key, nodeId, host, port })),
        };
      }
      const { nodeId, host, port } = parsed.coordinator;
      return { coordinators: [{ key: singleKey(options), nodeId, host, port }] };
    },
  },
);
