import type { ResponseDefinition } from '../../../schema.js'
import { apiVersionsResponseV1, type ApiVersionsResponseV1Body } from '../v1/response.js'

export type ApiVersionsResponseV2Body = Omit<ApiVersionsResponseV1Body, 'throttleTime'> & {
  throttleTime: number
  clientSideThrottleTime: number
}

/**
 * Starting in version 2, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v1; only the meaning of `throttleTime` changes (it becomes the
 * client-side wait, and the broker-side wait — always 0 for this API — is exposed separately).
 */
export const apiVersionsResponseV2: ResponseDefinition<ApiVersionsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = await apiVersionsResponseV1.decode(rawData)
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime }
  },
  parse: async (data) => {
    await apiVersionsResponseV1.parse(data)
    return data
  },
}
