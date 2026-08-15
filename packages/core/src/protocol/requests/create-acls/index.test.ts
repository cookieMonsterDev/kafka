import { describe, expect, it } from 'vitest';
import { CreateAcls } from './index.js';

describe('protocol/requests/create-acls', () => {
  it('implements only version 1 — the real Kafka 4.0.0 floor for this API', () => {
    expect(CreateAcls.versions).toEqual([1]);
  });
});
