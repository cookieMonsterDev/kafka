import { describe, expect, it } from 'vitest';
import { Metadata, type MetadataOptions } from './index';

describe('protocol/requests/metadata', () => {
  it('implements versions 0 through 13', () => {
    expect(Metadata.versions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);
  });

  it('defaults topics to [] and sends the given allowAutoTopicCreation', async () => {
    const allowed = await Metadata.protocol({ version: 4 })({ allowAutoTopicCreation: true }).request.encode();
    const denied = await Metadata.protocol({ version: 4 })({ allowAutoTopicCreation: false }).request.encode();
    // topics=[] on v4+ collapses to wire length -1, then the allowAutoTopicCreation byte
    expect(allowed.buffer).toEqual(Buffer.from([0xff, 0xff, 0xff, 0xff, 1]));
    expect(denied.buffer).toEqual(Buffer.from([0xff, 0xff, 0xff, 0xff, 0]));
  });

  it('requires allowAutoTopicCreation and never falls back to the wire default of true', async () => {
    // @ts-expect-error -- allowAutoTopicCreation is required
    const omitted: MetadataOptions = {};
    const encoder = await Metadata.protocol({ version: 4 })(omitted).request.encode();
    expect(encoder.buffer).toEqual(Buffer.from([0xff, 0xff, 0xff, 0xff, 0]));
  });

  it('encodes v9 defaults with compact null topics', async () => {
    const { request } = Metadata.protocol({ version: 9 })({ allowAutoTopicCreation: false });
    const encoder = await request.encode();
    // compact null topics, allowAutoTopicCreation=false, include flags false, TAG_BUFFER
    expect(encoder.buffer).toEqual(Buffer.from([0, 0, 0, 0, 0]));
  });

  it('encodes v10 defaults like v9 (still includes the cluster flag)', async () => {
    const { request } = Metadata.protocol({ version: 10 })({ allowAutoTopicCreation: true });
    const encoder = await request.encode();
    expect(encoder.buffer).toEqual(Buffer.from([0, 1, 0, 0, 0]));
  });

  it('encodes v11+ defaults without includeClusterAuthorizedOperations', async () => {
    const options = { allowAutoTopicCreation: true };
    const v11 = await Metadata.protocol({ version: 11 })(options).request.encode();
    const v12 = await Metadata.protocol({ version: 12 })(options).request.encode();
    const v13 = await Metadata.protocol({ version: 13 })(options).request.encode();
    expect(v11.buffer).toEqual(Buffer.from([0, 1, 0, 0]));
    expect(v12.buffer).toEqual(v11.buffer);
    expect(v13.buffer).toEqual(v11.buffer);
  });
});
