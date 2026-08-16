import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { CONFIG_RESOURCE_TYPES } from '../../../enums/config-resource-types';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { withDefaultConfigNames } from '../v1/request';
import { describeConfigsRequestV3, requestSchema } from './request';

const payload = {
  includeSynonyms: true,
  includeDocumentation: true,
  resources: withDefaultConfigNames([
    {
      type: CONFIG_RESOURCE_TYPES.TOPIC,
      name: 'topic-test1',
      configNames: ['compression.type', 'retention.ms'],
    },
  ]),
};

describe('protocol/requests/describe-configs/v3/request', () => {
  it('encodes v1 plus include_documentation', async () => {
    const definition = describeConfigsRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const expected = Buffer.concat([Buffer.from(v1RequestFixture.data), Buffer.from([1])]);
    expect(encoder.buffer).toEqual(expected);
  });

  it('defaults include_documentation to a trailing false byte relative to v1', async () => {
    const encoder = await describeConfigsRequestV3({ ...payload, includeDocumentation: false }).encode();
    const expected = Buffer.concat([Buffer.from(v1RequestFixture.data), Buffer.from([0])]);
    expect(encoder.buffer).toEqual(expected);
  });

  it('round-trips includeDocumentation', async () => {
    const encoder = await describeConfigsRequestV3(payload).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
