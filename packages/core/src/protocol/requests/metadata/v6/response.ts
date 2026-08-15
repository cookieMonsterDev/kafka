import type { ResponseDefinition } from '../../../schema.js'
import { metadataResponseV5 } from '../v5/response.js'

type MetadataResponseV5Body = Awaited<ReturnType<typeof metadataResponseV5.decode>>
export type MetadataResponseV6Body = Omit<MetadataResponseV5Body, 'throttleTime'> & {
  throttleTime: number
  clientSideThrottleTime: number
}

/**
 * In version 6, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v5; only the meaning of `throttleTime` changes.
 */
export const metadataResponseV6: ResponseDefinition<MetadataResponseV6Body> = {
  decode: async (rawData) => {
    const decoded = await metadataResponseV5.decode(rawData)
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime }
  },
  parse: async (data) => {
    await metadataResponseV5.parse(data)
    return data
  },
}
