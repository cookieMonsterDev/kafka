import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { OffsetFetch, committedOffsetsOf, type OffsetFetchOptions } from './index';
import { requestSchema as requestSchemaV8 } from './v8/request';
import { requestSchema as requestSchemaV9 } from './v9/request';
import { requestSchema as requestSchemaV10 } from './v10/request';

const ordersId = Buffer.alloc(16, 1);

async function encoded(version: number, options: OffsetFetchOptions): Promise<Buffer> {
  const { request } = OffsetFetch.protocol({ version })(options);
  return (await request.encode()).buffer;
}

async function parsed(version: number, options: OffsetFetchOptions, body: unknown): Promise<unknown> {
  const { response } = OffsetFetch.protocol({ version })(options);
  return OffsetFetch.parseResponse?.(body, options, (versionBody) => response.parse(versionBody));
}

describe('protocol/requests/offset-fetch', () => {
  it('implements versions 1 through 10', () => {
    expect(OffsetFetch.versions).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  describe('request', () => {
    const options: OffsetFetchOptions = {
      groups: [
        {
          groupId: 'g1',
          memberId: 'm1',
          memberEpoch: 4,
          topics: [{ topic: 'orders', topicId: ordersId, partitions: [{ partition: 0 }] }],
        },
        { groupId: 'g2', topics: [] },
      ],
    };

    it('takes exactly one group before v8', async () => {
      await expect(encoded(7, options)).rejects.toThrow('OffsetFetch before v8 fetches exactly one group');
      await expect(encoded(7, { groups: [] })).rejects.toThrow('OffsetFetch before v8 fetches exactly one group');
    });

    it('sends several groups from v8, without the member fields', async () => {
      expect(requestSchemaV8.read(new Decoder(await encoded(8, options)))).toEqual({
        groups: [
          { groupId: 'g1', topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }] },
          { groupId: 'g2', topics: [] },
        ],
        requireStable: false,
      });
    });

    it('sends each group member id and epoch from v9, defaulting to null and -1', async () => {
      expect(requestSchemaV9.read(new Decoder(await encoded(9, { ...options, requireStable: true })))).toEqual({
        groups: [
          {
            groupId: 'g1',
            memberId: 'm1',
            memberEpoch: 4,
            topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }],
          },
          { groupId: 'g2', memberId: null, memberEpoch: -1, topics: [] },
        ],
        requireStable: true,
      });
    });

    it('sends topic ids from v10', async () => {
      const [group] = options.groups;
      expect(requestSchemaV10.read(new Decoder(await encoded(10, { groups: [group!] })))).toEqual({
        groups: [
          {
            groupId: 'g1',
            memberId: 'm1',
            memberEpoch: 4,
            topics: [{ topicId: ordersId, partitions: [{ partition: 0 }] }],
          },
        ],
        requireStable: false,
      });
    });

    it('builds a v10 request for a topic without an id, but refuses to encode it', () => {
      // Broker#send builds the negotiated version before preferVersion picks v9 for such a topic.
      const { request } = OffsetFetch.protocol({ version: 10 })({
        groups: [{ groupId: 'g', topics: [{ topic: 'orders', partitions: [{ partition: 0 }] }] }],
      });
      expect(() => request.encode()).toThrow('OffsetFetch v10 requires a 16-byte topicId for topic orders');
    });
  });

  describe('preferVersion', () => {
    const advertised = { minVersion: 1, maxVersion: 10 };
    const prefer = (options: OffsetFetchOptions, version = 10): number | undefined =>
      OffsetFetch.preferVersion?.(options, version, advertised);

    it('keeps v10 when every requested topic has a usable id', () => {
      expect(
        prefer({ groups: [{ groupId: 'g', topics: [{ topic: 'orders', topicId: ordersId, partitions: [] }] }] }),
      ).toBe(10);
    });

    it('falls back to v9 for a topic without an id, or a group that asks for all of its topics', () => {
      expect(prefer({ groups: [{ groupId: 'g', topics: [{ topic: 'orders', partitions: [] }] }] })).toBe(9);
      expect(
        prefer({
          groups: [{ groupId: 'g', topics: [{ topic: 'orders', topicId: Buffer.alloc(16), partitions: [] }] }],
        }),
      ).toBe(9);
      expect(prefer({ groups: [{ groupId: 'g', topics: [] }] })).toBe(9);
    });

    it('leaves a version below 10 alone', () => {
      expect(prefer({ groups: [{ groupId: 'g', topics: [] }] }, 7)).toBe(7);
    });
  });

  describe('parseResponse', () => {
    const single: OffsetFetchOptions = { groups: [{ groupId: 'g1', topics: [] }] };

    it('names the one group of a response before v8, and fills an unknown leader epoch', async () => {
      await expect(
        parsed(1, single, {
          responses: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n, metadata: null, errorCode: 0 }] }],
        }),
      ).resolves.toEqual({
        groups: [
          {
            groupId: 'g1',
            topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n, leaderEpoch: -1, metadata: null }] }],
          },
        ],
      });
    });

    it('keeps every group of a v8+ response and drops error codes once they are checked', async () => {
      const options: OffsetFetchOptions = {
        groups: [
          { groupId: 'g1', topics: [] },
          { groupId: 'g2', topics: [] },
        ],
      };
      await expect(
        parsed(8, options, {
          throttleTime: 0,
          clientSideThrottleTime: 0,
          groups: [
            {
              groupId: 'g1',
              errorCode: 0,
              topics: [
                {
                  topic: 'orders',
                  partitions: [{ partition: 0, offset: 5n, leaderEpoch: 2, metadata: 'm', errorCode: 0 }],
                },
              ],
            },
            { groupId: 'g2', errorCode: 0, topics: [] },
          ],
        }),
      ).resolves.toEqual({
        groups: [
          {
            groupId: 'g1',
            topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n, leaderEpoch: 2, metadata: 'm' }] }],
          },
          { groupId: 'g2', topics: [] },
        ],
      });
    });

    it('throws a group-level error from v8', async () => {
      await expect(
        parsed(9, single, {
          throttleTime: 0,
          clientSideThrottleTime: 0,
          groups: [{ groupId: 'g1', errorCode: 113, topics: [] }],
        }),
      ).rejects.toMatchObject({ type: 'STALE_MEMBER_EPOCH' });
    });

    it('restores v10 topic names from the request', async () => {
      const options: OffsetFetchOptions = {
        groups: [{ groupId: 'g1', topics: [{ topic: 'orders', topicId: ordersId, partitions: [{ partition: 0 }] }] }],
      };
      const { response } = OffsetFetch.protocol({ version: 10 })(options);
      const body = await response.decode(
        new Encoder()
          .writeInt32(0)
          .writeUVarInt(2)
          .writeUVarIntString('g1')
          .writeUVarInt(2)
          .writeBuffer(ordersId)
          .writeUVarInt(2)
          .writeInt32(0)
          .writeInt64(9n)
          .writeInt32(3)
          .writeUVarIntString(null)
          .writeInt16(0)
          .writeUVarInt(0)
          .writeUVarInt(0)
          .writeInt16(0)
          .writeUVarInt(0)
          .writeUVarInt(0).buffer,
      );

      await expect(OffsetFetch.parseResponse?.(body, options, (b) => response.parse(b as never))).resolves.toEqual({
        groups: [
          {
            groupId: 'g1',
            topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 9n, leaderEpoch: 3, metadata: null }] }],
          },
        ],
      });
    });
  });

  describe('committedOffsetsOf', () => {
    it("returns the group's topics, or none for a group the response leaves out", () => {
      const topics = [{ topic: 'orders', partitions: [] }];
      const response = { groups: [{ groupId: 'g1', topics }] };
      expect(committedOffsetsOf(response, 'g1')).toBe(topics);
      expect(committedOffsetsOf(response, 'g2')).toEqual([]);
    });
  });
});
