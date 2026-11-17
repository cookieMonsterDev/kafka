import { vi, type Mock } from 'vitest';
import type { KafkaApi } from '../protocol/requests/index';
import { LeaveGroupMembers } from '../protocol/requests/leave-group/index';
import type { MetadataOptions } from '../protocol/requests/metadata/index';

/** The stub a unit-test fake defines for a family: its API's name in camel case, as in `createTopics`. */
function stubName(family: KafkaApi): string {
  if (family === LeaveGroupMembers) return 'leaveGroupMembers';
  if (family.apiName === 'GroupCoordinator') return 'findGroupCoordinator';
  return family.apiName.charAt(0).toLowerCase() + family.apiName.slice(1);
}

/**
 * Gives a unit-test fake broker a `send` that calls the fake's stub for the family's API, so a test
 * stubs only the APIs it uses, such as `createTopics: vi.fn()` for CreateTopics. A `metadata` stub
 * receives the topic list. Returns the same object. Not built into the package (`tsconfig.build.json`).
 */
export function withSend<Fake extends object>(fake: Fake): Fake & { send: Mock } {
  const send = vi.fn((family: KafkaApi, options: unknown): unknown => {
    const name = stubName(family);
    const stub = (fake as Record<string, unknown>)[name];
    if (typeof stub !== 'function') {
      return Promise.reject(new Error(`The fake broker has no ${name} stub`));
    }
    const stubOptions = name === 'metadata' ? (options as MetadataOptions).topics : options;
    return (stub as (options: unknown) => unknown).call(fake, stubOptions);
  });
  return Object.assign(fake, { send });
}
