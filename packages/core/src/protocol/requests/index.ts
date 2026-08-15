import { KafkaJSNotImplemented, KafkaJSServerDoesNotSupportApiKey } from '../../errors.js'
import type { Encoder } from '../encoder.js'
import { apiKeyName } from './api-keys.js'

export interface AnyRequestDefinition {
  apiKey: number
  apiVersion: number
  apiName: string
  encode(): Promise<Encoder>
}

export interface AnyResponseDefinition {
  decode(rawData: Buffer): Promise<unknown>
  parse(data: unknown): Promise<unknown>
}

export interface ProtocolResult {
  request: AnyRequestDefinition
  response: AnyResponseDefinition
  logResponseError?: boolean
}

/**
 * The per-version factory a family's `protocol({version})` resolves to — e.g. ApiVersions's is
 * zero-arg (`() => ProtocolResult`), Metadata's takes `{topics, allowAutoTopicCreation}`. Kept
 * loosely typed at this generic dispatch layer (matching kafkajs's own untyped JS here); each
 * family's concrete exports (e.g. `apiVersionsRequestV0`) stay fully typed and are what Phase 4's
 * broker layer calls directly.
 */
export type ProtocolFactory = (...args: never[]) => ProtocolResult

export interface RequestFamily {
  readonly versions: readonly number[]
  protocol(options: { version: number }): ProtocolFactory
}

export const NOT_IMPLEMENTED_REQUEST_DEFINITIONS: RequestFamily = Object.freeze({
  versions: Object.freeze([]),
  protocol(): never {
    throw new KafkaJSNotImplemented('This API is not implemented')
  },
})

/**
 * `lookup(brokerVersions)(apiKey, family)` picks `min(highest version we implement, highest the
 * broker advertised)` and returns that version's factory, or throws if the broker never
 * advertised the API at all.
 */
export function lookup(
  brokerVersions: Readonly<Record<number, { maxVersion: number } | undefined>>
): (apiKey: number, family: RequestFamily) => ProtocolFactory {
  return (apiKey, family) => {
    const version = brokerVersions[apiKey]
    if (version == null || version.maxVersion == null) {
      throw new KafkaJSServerDoesNotSupportApiKey('The Kafka server does not support the requested API version', {
        apiKey,
        apiName: apiKeyName(apiKey),
      })
    }

    const bestImplementedVersion = Math.max(...family.versions)
    const bestSupportedVersion = Math.min(bestImplementedVersion, version.maxVersion)
    return family.protocol({ version: bestSupportedVersion })
  }
}
