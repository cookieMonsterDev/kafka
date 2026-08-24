import { afterEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { InstrumentationEventEmitter } from '../../../src/instrumentation/emitter';
import { createCluster, newLogger } from '../../helpers/index';

describe('admin.instrumentation', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  it('emits connect, request, and disconnect', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter();
    admin = createAdmin({
      cluster: createCluster({ instrumentationEmitter }),
      instrumentationEmitter,
      logger: newLogger(),
    });
    const events: string[] = [];
    admin.on(admin.events.CONNECT, () => {
      events.push('connect');
    });
    admin.on(admin.events.DISCONNECT, () => {
      events.push('disconnect');
    });
    admin.on(admin.events.REQUEST, () => {
      events.push('request');
    });

    await admin.connect();
    await admin.listTopics();
    await admin.disconnect();
    admin = undefined;

    expect(events).toEqual(expect.arrayContaining(['connect', 'request', 'disconnect']));
  });

  it('rejects an unknown event name', () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    expect(() => admin!.on('not-an-event' as never, () => undefined)).toThrow(/Event name should be one of/);
  });

  it('connects after an aborted signal is not reused on the next call', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await expect(admin.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    await expect(admin.connect()).resolves.toBeUndefined();
    expect(await admin.listTopics()).toEqual(expect.any(Array));
    expect(typeof admin.logger().info).toBe('function');
  });
});
