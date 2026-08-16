import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { MemberAssignment, MemberMetadata } from './assigner-protocol';

const fixturesDir = join(dirname(fileURLToPath(import.meta.url)), 'fixtures/round-robin-assigner');

function fixtureBuffer(name: string): Buffer {
  const json = JSON.parse(readFileSync(join(fixturesDir, name), 'utf8')) as { data: number[] };
  return Buffer.from(json.data);
}

describe('consumer/assigner-protocol', () => {
  describe('MemberMetadata', () => {
    it('encodes', () => {
      const buffer = MemberMetadata.encode({ version: 1, topics: ['topic-test'] });
      expect(buffer).toEqual(fixtureBuffer('member-metadata.json'));
    });

    it('decodes', () => {
      expect(MemberMetadata.decode(fixtureBuffer('member-metadata.json'))).toEqual({
        version: 1,
        topics: ['topic-test'],
        userData: Buffer.alloc(0),
      });
    });
  });

  describe('MemberAssignment', () => {
    it('encodes', () => {
      const buffer = MemberAssignment.encode({
        version: 1,
        assignment: { 'topic-test': [2, 5, 4, 1, 3, 0] },
      });
      expect(buffer).toEqual(fixtureBuffer('member-assignment.json'));
    });

    it('decodes', () => {
      expect(MemberAssignment.decode(fixtureBuffer('member-assignment.json'))).toEqual({
        version: 1,
        assignment: { 'topic-test': [2, 5, 4, 1, 3, 0] },
        userData: Buffer.alloc(0),
      });
    });

    it('decodes an empty assignment as null', () => {
      expect(MemberAssignment.decode(Buffer.from([]))).toBe(null);
    });
  });
});
