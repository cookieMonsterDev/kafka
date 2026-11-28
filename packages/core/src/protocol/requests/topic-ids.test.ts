import { describe, expect, it } from 'vitest';
import { isUsableTopicId, requestedTopicName } from './topic-ids';

describe('protocol/requests/topic-ids', () => {
  describe('isUsableTopicId', () => {
    it('is true only for a non-zero 16-byte UUID', () => {
      expect(isUsableTopicId(undefined)).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(0))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(15, 1))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(16))).toBe(false);
      expect(isUsableTopicId(Buffer.alloc(16, 1))).toBe(true);
    });
  });

  describe('requestedTopicName', () => {
    const ordersId = Buffer.alloc(16, 1);
    const paymentsId = Buffer.alloc(16, 2);
    const topics = [
      { topic: 'orders', topicId: ordersId },
      { topic: 'payments', topicId: paymentsId },
    ];

    it('resolves by topic UUID, then by index, then to an empty string', () => {
      expect(requestedTopicName(paymentsId, 0, topics)).toBe('payments');
      expect(requestedTopicName(Buffer.alloc(16, 9), 1, topics)).toBe('payments');
      expect(requestedTopicName(Buffer.alloc(16, 9), 99, topics)).toBe('');
      expect(requestedTopicName(Buffer.alloc(16, 9), 0, [{ topic: 'anon' }])).toBe('anon');
    });
  });
});
