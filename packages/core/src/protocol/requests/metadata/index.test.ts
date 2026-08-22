import { describe, expect, it } from 'vitest';
import { Metadata } from './index';

describe('protocol/requests/metadata', () => {
  it('implements versions 0 through 13', () => {
    expect(Metadata.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
  });

  it('defaults topics to [] and allowAutoTopicCreation to true', async () => {
    const { request } = Metadata.protocol({ version: 4 })({});
    const encoder = await request.encode();
    // topics=[] on v4+ collapses to wire length -1, then allowAutoTopicCreation=true (byte 1)
    expect(encoder.buffer).toEqual(Buffer.from([0xff, 0xff, 0xff, 0xff, 1]));
  });

  it('encodes v9 defaults with compact null topics', async () => {
    const { request } = Metadata.protocol({ version: 9 })({});
    const encoder = await request.encode();
    // compact null topics, allowAutoTopicCreation=true, include flags false, TAG_BUFFER
    expect(encoder.buffer).toEqual(Buffer.from([0, 1, 0, 0, 0]));
  });

  it('encodes v10 defaults like v9 (still includes the cluster flag)', async () => {
    const { request } = Metadata.protocol({ version: 10 })({});
    const encoder = await request.encode();
    expect(encoder.buffer).toEqual(Buffer.from([0, 1, 0, 0, 0]));
  });

  it('encodes v11+ defaults without includeClusterAuthorizedOperations', async () => {
    const v11 = await Metadata.protocol({ version: 11 })({}).request.encode();
    const v12 = await Metadata.protocol({ version: 12 })({}).request.encode();
    const v13 = await Metadata.protocol({ version: 13 })({}).request.encode();
    expect(v11.buffer).toEqual(Buffer.from([0, 1, 0, 0]));
    expect(v12.buffer).toEqual(v11.buffer);
    expect(v13.buffer).toEqual(v11.buffer);
  });
});
