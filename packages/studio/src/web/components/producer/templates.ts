/** One quick-start payload a producer form can fill itself with, plus the means to re-roll it. */
export interface PayloadTemplate {
  readonly id: string;
  readonly label: string;
  readonly description: string;
  build(): { readonly key: string; readonly value: string };
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const SAMPLE_NAMES = ['ava', 'noah', 'mia', 'leo', 'zoe', 'kai'] as const;

function randomEmail(): string {
  const name = SAMPLE_NAMES[randomInt(0, SAMPLE_NAMES.length - 1)];
  return `${name}${String(randomInt(1, 999))}@example.com`;
}

export const PAYLOAD_TEMPLATES: readonly PayloadTemplate[] = [
  {
    id: 'user-created',
    label: 'User created event',
    description: 'A user.created domain event, keyed by a fresh user id.',
    build: () => {
      const userId = crypto.randomUUID();
      return {
        key: userId,
        value: JSON.stringify(
          { type: 'user.created', userId, email: randomEmail(), createdAt: new Date().toISOString() },
          null,
          2,
        ),
      };
    },
  },
  {
    id: 'order-placed',
    label: 'Order placed event',
    description: 'An order.placed domain event with a random total, keyed by a fresh order id.',
    build: () => {
      const orderId = crypto.randomUUID();
      return {
        key: orderId,
        value: JSON.stringify(
          {
            type: 'order.placed',
            orderId,
            totalCents: randomInt(500, 25_000),
            currency: 'USD',
            placedAt: new Date().toISOString(),
          },
          null,
          2,
        ),
      };
    },
  },
  {
    id: 'sequence',
    label: 'Sequence marker',
    description: 'A tiny payload with the {{seq}} placeholder a burst substitutes per message.',
    build: () => ({ key: 'seq-{{seq}}', value: JSON.stringify({ seq: '{{seq}}' }) }),
  },
  {
    id: 'empty',
    label: 'Empty object',
    description: 'A blank {} payload with no key, for a quick smoke test.',
    build: () => ({ key: '', value: '{}' }),
  },
];
