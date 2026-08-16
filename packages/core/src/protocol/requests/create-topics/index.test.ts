import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { CreateTopics } from './index';

describe('protocol/requests/create-topics', () => {
  it('implements versions 0 through 3', () => {
    expect(CreateTopics.versions).toEqual([0, 1, 2, 3]);
  });

  it('throws when validateOnly is set on a v0 broker', () => {
    expect(() =>
      CreateTopics.protocol({ version: 0 })({
        topics: [{ topic: 't' }],
        validateOnly: true,
      }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('allows validateOnly to be omitted on v0', () => {
    expect(() => CreateTopics.protocol({ version: 0 })({ topics: [{ topic: 't' }] })).not.toThrow();
  });
});
