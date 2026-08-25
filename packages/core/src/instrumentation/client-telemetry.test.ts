import { afterEach, describe, expect, it, vi } from 'vitest';
import { API_KEYS } from '../protocol/requests/api-keys';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { createErrorFromCode } from '../protocol/error-codes';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { ClientTelemetryReporter } from './client-telemetry';
import { TelemetrySnapshot } from './telemetry-snapshot';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function subscription(overrides: Record<string, unknown> = {}) {
  return {
    throttleTime: 0,
    clientSideThrottleTime: 0,
    errorCode: 0,
    clientInstanceId: Buffer.alloc(16, 9),
    subscriptionId: 1,
    acceptedCompressionTypes: [0],
    pushIntervalMs: 60_000,
    telemetryMaxBytes: 10_000,
    deltaTemporality: true,
    requestedMetrics: [''],
    ...overrides,
  };
}

describe('instrumentation/client-telemetry', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('subscribes, pushes OTLP metrics, and sends a terminating push on stop', async () => {
    const assigned = Buffer.alloc(16, 9);
    const getTelemetrySubscriptions = vi.fn().mockResolvedValue(subscription());
    const pushTelemetry = vi.fn().mockResolvedValue({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 0 });
    const broker = {
      versions: { [API_KEYS.GetTelemetrySubscriptions]: { maxVersion: 0 } },
      getTelemetrySubscriptions,
      pushTelemetry,
    };
    const snapshot = new TelemetrySnapshot();
    snapshot.recordProduce({ records: 2, bytes: 8, retries: 1 });
    const reporter = new ClientTelemetryReporter({
      getBroker: async () => broker as never,
      logger: silentLogger,
      clientId: 'telemetry-test',
      snapshot,
    });

    reporter.start();
    await vi.waitFor(() => {
      expect(pushTelemetry).toHaveBeenCalled();
    });
    expect(reporter.clientInstanceId()).toEqual(assigned);
    expect(getTelemetrySubscriptions).toHaveBeenCalledWith({ clientInstanceId: Buffer.alloc(16) });
    expect(pushTelemetry.mock.calls[0]?.[0]).toMatchObject({
      clientInstanceId: assigned,
      subscriptionId: 1,
      terminating: false,
      compressionType: 0,
    });
    expect(Buffer.isBuffer(pushTelemetry.mock.calls[0]?.[0].metrics)).toBe(true);
    expect(pushTelemetry.mock.calls[0]?.[0].metrics.length).toBeGreaterThan(0);

    await reporter.stop();
    const last = pushTelemetry.mock.calls.at(-1)?.[0] as { terminating: boolean };
    expect(last.terminating).toBe(true);
  });

  it('does not push when the broker requested no metrics', async () => {
    const pushTelemetry = vi.fn();
    const broker = {
      versions: { [API_KEYS.GetTelemetrySubscriptions]: { maxVersion: 0 } },
      getTelemetrySubscriptions: vi.fn().mockResolvedValue(subscription({ requestedMetrics: [] })),
      pushTelemetry,
    };
    const reporter = new ClientTelemetryReporter({
      getBroker: async () => broker as never,
      logger: silentLogger,
      clientId: 'c',
      snapshot: new TelemetrySnapshot(),
    });

    reporter.start();
    await vi.waitFor(() => {
      expect(reporter.clientInstanceId()).toEqual(Buffer.alloc(16, 9));
    });
    expect(pushTelemetry).not.toHaveBeenCalled();
    await reporter.stop();
  });

  it('disables itself when the broker does not advertise telemetry APIs', async () => {
    const getTelemetrySubscriptions = vi.fn();
    const broker = { versions: {}, getTelemetrySubscriptions };
    const reporter = new ClientTelemetryReporter({
      getBroker: async () => broker as never,
      logger: silentLogger,
      clientId: 'c',
      snapshot: new TelemetrySnapshot(),
    });

    reporter.start();
    await vi.waitFor(() => {
      expect(reporter.clientInstanceId()).toBeNull();
    });
    expect(getTelemetrySubscriptions).not.toHaveBeenCalled();
    await reporter.stop();
  });

  it('gzip-compresses PushTelemetry when the broker accepts gzip and the payload is large', async () => {
    const pushTelemetry = vi.fn().mockResolvedValue({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 0 });
    const broker = {
      versions: { [API_KEYS.GetTelemetrySubscriptions]: { maxVersion: 0 } },
      getTelemetrySubscriptions: vi.fn().mockResolvedValue(
        subscription({
          acceptedCompressionTypes: [COMPRESSION_TYPES.GZIP],
          telemetryMaxBytes: 10_000,
          requestedMetrics: [''],
        }),
      ),
      pushTelemetry,
    };
    const snapshot = new TelemetrySnapshot();
    vi.spyOn(snapshot, 'encode').mockReturnValue(Buffer.alloc(600, 1));
    const reporter = new ClientTelemetryReporter({
      getBroker: async () => broker as never,
      logger: silentLogger,
      clientId: 'c',
      snapshot,
    });

    reporter.start();
    await vi.waitFor(() => {
      expect(pushTelemetry).toHaveBeenCalled();
    });
    expect(pushTelemetry.mock.calls[0]?.[0].compressionType).toBe(COMPRESSION_TYPES.GZIP);
    expect(pushTelemetry.mock.calls[0]?.[0].metrics.length).toBeLessThan(600);
    await reporter.stop();
  });

  it('skips PushTelemetry when the payload exceeds telemetryMaxBytes', async () => {
    const pushTelemetry = vi.fn();
    const broker = {
      versions: { [API_KEYS.GetTelemetrySubscriptions]: { maxVersion: 0 } },
      getTelemetrySubscriptions: vi.fn().mockResolvedValue(
        subscription({
          acceptedCompressionTypes: [0],
          telemetryMaxBytes: 50,
          requestedMetrics: [''],
        }),
      ),
      pushTelemetry,
    };
    const snapshot = new TelemetrySnapshot();
    vi.spyOn(snapshot, 'encode').mockReturnValue(Buffer.alloc(200));
    const reporter = new ClientTelemetryReporter({
      getBroker: async () => broker as never,
      logger: silentLogger,
      clientId: 'c',
      snapshot,
    });

    reporter.start();
    await vi.waitFor(() => {
      expect(reporter.clientInstanceId()).toEqual(Buffer.alloc(16, 9));
    });
    expect(pushTelemetry).not.toHaveBeenCalled();
    await reporter.stop();
  });

  it('swallows TELEMETRY_TOO_LARGE from the broker', async () => {
    const pushTelemetry = vi.fn().mockRejectedValue(createErrorFromCode(118));
    const broker = {
      versions: { [API_KEYS.GetTelemetrySubscriptions]: { maxVersion: 0 } },
      getTelemetrySubscriptions: vi.fn().mockResolvedValue(subscription()),
      pushTelemetry,
    };
    const reporter = new ClientTelemetryReporter({
      getBroker: async () => broker as never,
      logger: silentLogger,
      clientId: 'c',
      snapshot: new TelemetrySnapshot(),
    });

    reporter.start();
    await vi.waitFor(() => {
      expect(pushTelemetry).toHaveBeenCalled();
    });
    await expect(reporter.stop()).resolves.toBeUndefined();
  });
});
