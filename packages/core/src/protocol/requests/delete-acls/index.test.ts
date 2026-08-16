import { describe, expect, it } from 'vitest';
import { DeleteAcls } from './index';

describe('protocol/requests/delete-acls', () => {
  it('implements only version 1 — the real Kafka 4.0.0 floor for this API', () => {
    expect(DeleteAcls.versions).toEqual([1]);
  });
});
