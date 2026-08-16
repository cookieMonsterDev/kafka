import { createSaslAuthenticator } from '../broker/sasl-authenticator/index';
import { KafkaConnectionError, KafkaNonRetriableError } from '../errors';
import type { Logger } from '../loggers/index';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { ConnectionPool } from '../network/connection-pool';
import type { ConnectionOptions } from '../network/connection';
import type { NetworkEventMap } from '../network/instrumentation-events';
import type { SocketFactory } from '../network/socket-factory';
import { parseBrokerAddress } from './parse-broker-address';

export interface ConnectionPoolBuilderOptions {
  /** Socket factory; the public `Kafka` client supplies the built-in default. */
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
  instrumentationEmitter?: InstrumentationEventEmitter | null;
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
    throw new KafkaNonRetriableError('Failed to connect: brokers should not be null');
  }

  if (!brokers.length) {
    throw new KafkaNonRetriableError('Failed to connect: brokers array is empty');
  }

  brokers.forEach((broker, index) => {
    if (!isValidBroker(broker)) {
      throw new KafkaNonRetriableError(`Failed to connect: broker at index ${index} is invalid "${typeof broker}"`);
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
        const wrappedError = new KafkaConnectionError(
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
        const parsed = parseBrokerAddress(randomBroker);
        host = parsed.host;
        port = parsed.port;
      }

      return new ConnectionPool({
        host: host,
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
        instrumentationEmitter: instrumentationEmitter as
          InstrumentationEventEmitter<NetworkEventMap> | null | undefined,
        logger,
        reauthenticationThreshold,
        createSaslAuthenticator,
      });
    },
  };
}
