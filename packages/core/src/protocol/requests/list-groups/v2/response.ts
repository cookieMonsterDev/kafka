import type { ResponseDefinition } from '../../../schema.js'
import { listGroupsResponseV1 } from '../v1/response.js'

export interface ListGroupsResponseV2Body {
  errorCode: number
  groups: { groupId: string; protocolType: string }[]
  throttleTime: number
  clientSideThrottleTime: number
}

/**
 * Starting in version 2, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v1; only the meaning of `throttleTime` changes.
 */
export const listGroupsResponseV2: ResponseDefinition<ListGroupsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = await listGroupsResponseV1.decode(rawData)
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime }
  },
  parse: async (data) => {
    await listGroupsResponseV1.parse(data)
    return data
  },
}
