import { describe, expect, it } from 'vitest';
import { PAYLOAD_TEMPLATES } from './templates';

describe('PAYLOAD_TEMPLATES', () => {
  it('has a unique id for every template', () => {
    const ids = PAYLOAD_TEMPLATES.map((template) => template.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('builds a value for every template', () => {
    for (const template of PAYLOAD_TEMPLATES) {
      const built = template.build();
      expect(typeof built.key).toBe('string');
      expect(typeof built.value).toBe('string');
    }
  });

  it('every JSON-shaped template value parses as JSON', () => {
    for (const template of PAYLOAD_TEMPLATES) {
      expect(() => JSON.parse(template.build().value)).not.toThrow();
    }
  });

  it('the user-created template varies its key across builds', () => {
    const template = PAYLOAD_TEMPLATES.find((entry) => entry.id === 'user-created');
    expect(template).toBeDefined();
    const first = template?.build();
    const second = template?.build();
    expect(first?.key).not.toBe(second?.key);
  });

  it('the empty template has no key', () => {
    const template = PAYLOAD_TEMPLATES.find((entry) => entry.id === 'empty');
    expect(template?.build()).toEqual({ key: '', value: '{}' });
  });
});
