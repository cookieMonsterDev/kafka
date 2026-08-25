import type { Broker } from '../broker/index';
import { supportsClientTelemetry } from '../broker/capabilities';
import { KafkaBrokerNotFound, KafkaProtocolError, KafkaServerDoesNotSupportApiKey } from '../errors';
import type { Logger } from '../loggers/index';
import { COMPRESSION_TYPES, lookupCodec } from '../protocol/compression/index';
import { Encoder } from '../protocol/encoder';
import { ZERO_CLIENT_INSTANCE_ID } from '../protocol/requests/get-telemetry-subscriptions/index';
import type { GetTelemetrySubscriptionsResponseV0Body } from '../protocol/requests/get-telemetry-subscriptions/v0/response';
import type { ProduceMetrics } from './metrics';
import type { TelemetrySnapshot } from './telemetry-snapshot';

const SOFTWARE_NAME = '@cookiemonsterdev/kafka-core';
const SOFTWARE_VERSION = '1.2.0';
const DEFAULT_PUSH_INTERVAL_MS = 300_000;

export interface ClientTelemetryReporterOptions {
  getBroker: () => Promise<Broker>;
  logger: Logger;
  clientId: string;
  snapshot: TelemetrySnapshot;
}

/**
 * KIP-714: subscribe via GetTelemetrySubscriptions, then PushTelemetry on the broker interval.
 * Disabled automatically when the broker does not advertise API 71.
 */
export class ClientTelemetryReporter {
  readonly #getBroker: () => Promise<Broker>;
  readonly #logger: Logger;
  readonly #clientId: string;
  readonly #snapshot: TelemetrySnapshot;
  #clientInstanceId: Buffer | null = null;
  #subscription: GetTelemetrySubscriptionsResponseV0Body | null = null;
  #timer: NodeJS.Timeout | null = null;
  #stopped = true;
  #running: Promise<void> = Promise.resolve();

  constructor({ getBroker, logger, clientId, snapshot }: ClientTelemetryReporterOptions) {
    this.#getBroker = getBroker;
    this.#logger = logger.namespace('Telemetry');
    this.#clientId = clientId;
    this.#snapshot = snapshot;
  }

  clientInstanceId(): Buffer | null {
    return this.#clientInstanceId;
  }

  recordProduce(metrics: ProduceMetrics): void {
    this.#snapshot.recordProduce(metrics);
  }

  start(): void {
    if (!this.#stopped) return;
    this.#stopped = false;
    this.#running = this.#tick(false);
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    if (this.#timer) {
      clearTimeout(this.#timer);
      this.#timer = null;
    }
    await this.#running.catch(() => undefined);
    if (this.#subscription && this.#clientInstanceId) {
      await this.#push(true).catch((error: unknown) => {
        this.#logger.debug('Terminating PushTelemetry failed', {
          error: error instanceof Error ? error.message : String(error),
        });
      });
    }
  }

  async #tick(terminating: boolean): Promise<void> {
    if (this.#stopped && !terminating) return;
    try {
      if (!this.#subscription) await this.#subscribe();
      if (!this.#subscription) return;
      if (this.#subscription.requestedMetrics.length > 0) {
        await this.#push(terminating);
      }
    } catch (error) {
      if (error instanceof KafkaServerDoesNotSupportApiKey || error instanceof KafkaBrokerNotFound) {
        this.#logger.debug('Client telemetry disabled', {
          error: error instanceof Error ? error.message : String(error),
        });
        this.#stopped = true;
        return;
      }
      if (error instanceof KafkaProtocolError && error.type === 'UNKNOWN_SUBSCRIPTION_ID') {
        this.#subscription = null;
        this.#schedule(DEFAULT_PUSH_INTERVAL_MS);
        return;
      }
      this.#logger.debug('Client telemetry tick failed', {
        error: error instanceof Error ? error.message : String(error),
      });
    }
    if (!this.#stopped) {
      this.#schedule(this.#subscription?.pushIntervalMs ?? DEFAULT_PUSH_INTERVAL_MS);
    }
  }

  #schedule(intervalMs: number): void {
    if (this.#stopped) return;
    if (this.#timer) clearTimeout(this.#timer);
    const wait = intervalMs > 0 ? intervalMs : DEFAULT_PUSH_INTERVAL_MS;
    this.#timer = setTimeout(() => {
      this.#running = this.#tick(false);
    }, wait);
    this.#timer.unref?.();
  }

  async #subscribe(): Promise<void> {
    const broker = await this.#getBroker();
    if (!broker.versions || !supportsClientTelemetry(broker.versions)) {
      this.#stopped = true;
      this.#logger.debug('Broker ApiVersions has no GetTelemetrySubscriptions; disabling push');
      return;
    }

    const response = await broker.getTelemetrySubscriptions({
      clientInstanceId: this.#clientInstanceId ?? ZERO_CLIENT_INSTANCE_ID,
    });

    if (!response.clientInstanceId.equals(ZERO_CLIENT_INSTANCE_ID)) {
      this.#clientInstanceId = response.clientInstanceId;
    }
    this.#subscription = response;
  }

  async #push(terminating: boolean): Promise<void> {
    const subscription = this.#subscription;
    const clientInstanceId = this.#clientInstanceId;
    if (!subscription || !clientInstanceId) return;
    if (subscription.requestedMetrics.length === 0 && !terminating) return;

    const payload = this.#snapshot.encode({
      requestedMetrics: subscription.requestedMetrics.length === 0 ? [''] : subscription.requestedMetrics,
      deltaTemporality: subscription.deltaTemporality,
      resourceAttributes: [
        { key: 'service.name', value: this.#clientId || SOFTWARE_NAME },
        { key: 'telemetry.sdk.language', value: 'nodejs' },
        { key: 'client.software.name', value: SOFTWARE_NAME },
        { key: 'client.software.version', value: SOFTWARE_VERSION },
      ],
      scopeName: SOFTWARE_NAME,
      scopeVersion: SOFTWARE_VERSION,
    });

    const { metrics, compressionType } = await compressMetrics(
      payload,
      subscription.acceptedCompressionTypes,
      subscription.telemetryMaxBytes,
    );

    if (metrics.length > subscription.telemetryMaxBytes && subscription.telemetryMaxBytes > 0) {
      this.#logger.debug('PushTelemetry payload exceeds telemetryMaxBytes; skipping', {
        size: metrics.length,
        telemetryMaxBytes: subscription.telemetryMaxBytes,
      });
      return;
    }

    const broker = await this.#getBroker();
    try {
      await broker.pushTelemetry({
        clientInstanceId,
        subscriptionId: subscription.subscriptionId,
        terminating,
        compressionType,
        metrics,
      });
    } catch (error) {
      if (error instanceof KafkaProtocolError && error.type === 'TELEMETRY_TOO_LARGE') {
        this.#logger.debug('Broker rejected PushTelemetry as TELEMETRY_TOO_LARGE');
        return;
      }
      throw error;
    }
  }
}

async function compressMetrics(
  payload: Buffer,
  accepted: readonly number[],
  maxBytes: number,
): Promise<{ metrics: Buffer; compressionType: number }> {
  const gzipOk = accepted.includes(COMPRESSION_TYPES.GZIP);
  const overBudget = maxBytes > 0 && payload.length > maxBytes;
  if (!gzipOk || (!overBudget && payload.length < 512)) {
    return { metrics: payload, compressionType: COMPRESSION_TYPES.None };
  }

  const codec = lookupCodec(COMPRESSION_TYPES.GZIP);
  if (!codec) return { metrics: payload, compressionType: COMPRESSION_TYPES.None };

  const encoder = new Encoder();
  encoder.writeBuffer(payload);
  try {
    const compressed = await codec.compress(encoder);
    if (compressed.length < payload.length || overBudget) {
      return { metrics: compressed, compressionType: COMPRESSION_TYPES.GZIP };
    }
  } finally {
    encoder.release();
  }

  return { metrics: payload, compressionType: COMPRESSION_TYPES.None };
}
