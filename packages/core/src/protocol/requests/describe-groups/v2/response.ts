import type { ResponseDefinition } from '../../../schema.js'
import { describeGroupsResponseV1 } from '../v1/response.js'

type DescribeGroupsResponseV1Body = Awaited<ReturnType<typeof describeGroupsResponseV1.decode>>
export type DescribeGroupsResponseV2Body = Omit<DescribeGroupsResponseV1Body, 'throttleTime'> & {
  throttleTime: number
  clientSideThrottleTime: number
}

/**
 * Starting in version 2, on quota violation, brokers send the response before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Wire format is identical to v1; only the meaning of `throttleTime` changes.
 */
export const describeGroupsResponseV2: ResponseDefinition<DescribeGroupsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = await describeGroupsResponseV1.decode(rawData)
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime }
  },
  parse: async (data) => {
    await describeGroupsResponseV1.parse(data)
    return data
  },
}
