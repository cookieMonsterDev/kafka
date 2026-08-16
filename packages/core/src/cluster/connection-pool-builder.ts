import { createSaslAuthenticator } from '../broker/sasl-authenticator/index.js';
import { KafkaJSConnectionError, KafkaJSNonRetriableError } from '../errors.js';
import type { Logger } from '../loggers/index.js';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter.js';
import { ConnectionPool } from '../network/connection-pool.js';
import type { ConnectionOptions } from '../network/connection.js';
import type { NetworkEventMap } from '../network/instrumentation-events.js';
import type { SocketFactory } from '../network/socket-factory.js';

export interface ConnectionPoolBuilderOptions {
  /** Resolved to the built-in default one layer up (the public `Kafka` client), same as kafkajs's own layering. */
  socketFactory: SocketFactory;
  brokers: readonly string[] | (() => readonly string[] | Promise<readonly string[]>);
  ssl?: ConnectionOptions['ssl'];
  sasl?: ConnectionOptions['sasl'];
  clientId: string;
  requestTimeout: number;
  enforceRequestTimeout?: boolean;
  connectionTimeout: number;
  maxInFlightRequests?: number | null;
  logger: Logger;
  instrumentationEmitter?: InstrumentationEventEmitter<NetworkEventMap> | null;
  reauthenticationThreshold?: number;
}

export interface ConnectionPoolDestination {
  host?: string;
  port?: number;
  rack?: string | null;
}

export interface ConnectionPoolBuilder {
  build(destination?: ConnectionPoolDestination): Promise<ConnectionPool>;
}

function isValidBroker(broker: unknown): broker is string {
  return typeof broker === 'string' && broker.length > 0;
}

function validateBrokers(brokers: readonly string[] | null | undefined): asserts brokers is readonly string[] {
  if (!brokers) {
    throw new KafkaJSNonRetriableError('Failed to connect: brokers should not be null');
  }

  if (!brokers.length) {
    throw new KafkaJSNonRetriableError('Failed to connect: brokers array is empty');
  }

  brokers.forEach((broker, index) => {
    if (!isValidBroker(broker)) {
      throw new KafkaJSNonRetriableError(`Failed to connect: broker at index ${index} is invalid "${typeof broker}"`);
    }
  });
}

/** Builds a fresh `ConnectionPool` pointed at either an explicit destination or the next broker from the seed list. */
export function connectionPoolBuilder(options: ConnectionPoolBuilderOptions): ConnectionPoolBuilder {
  const {
    socketFactory,
    brokers,
    ssl,
    sasl,
    clientId,
    requestTimeout,
    enforceRequestTimeout,
    connectionTimeout,
    maxInFlightRequests,
    logger,
    instrumentationEmitter = null,
    reauthenticationThreshold,
  } = options;

  let index = 0;

  const getBrokers = async (): Promise<readonly string[]> => {
    let list: readonly string[];

    if (typeof brokers === 'function') {
      try {
        list = await brokers();
      } catch (e) {
        const wrappedError = new KafkaJSConnectionError(
          `Failed to connect: "config.brokers" threw: ${(e as Error).message}`,
        );
        wrappedError.stack = `${wrappedError.name}\n  Caused by: ${(e as Error).stack}`;
        throw wrappedError;
      }
    } else {
      list = brokers;
    }

    validateBrokers(list);
    return list;
  };

  return {
    build: async (destination: ConnectionPoolDestination = {}) => {
      let { host, port } = destination;
      const { rack } = destination;

      if (!host) {
        const list = await getBrokers();
        const randomBroker = list[index++ % list.length]!;
        const [brokerHost, brokerPort] = randomBroker.split(':');
        host = brokerHost;
        port = Number(brokerPort);
      }

      return new ConnectionPool({
        host: host!,
        port: port!,
        rack,
        sasl,
        ssl,
        clientId,
        socketFactory,
        connectionTimeout,
        requestTimeout,
        enforceRequestTimeout,
        maxInFlightRequests,
        instrumentationEmitter,
        logger,
        reauthenticationThreshold,
        createSaslAuthenticator,
      });
    },
  };
}
