import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { DateTimePicker } from './date-time-picker';
import { SelectInput } from './base-select';

describe('shadcn form controls', () => {
  it('renders the selected label rather than the internal dropdown value', () => {
    const html = renderToStaticMarkup(
      createElement(SelectInput, {
        value: 'json',
        onValueChange: () => {},
        'aria-label': 'Correlation value type',
        items: [
          { value: 'text', label: 'Text' },
          { value: 'json', label: 'JSON literal' },
        ],
      }),
    );
    expect(html).toContain('JSON literal');
    expect(html).toContain('aria-label="Correlation value type"');
    expect(html).not.toContain('<select');
  });
  it('keeps an empty-string option distinct from a missing selection', () => {
    const html = renderToStaticMarkup(
      createElement(SelectInput, {
        value: '',
        onValueChange: () => {},
        items: [
          { value: '', label: 'default' },
          { value: 'staging', label: 'Staging' },
        ],
      }),
    );
    expect(html).toContain('>default<');
  });
  it('renders a named calendar trigger with the local date and no native date-time input', () => {
    const html = renderToStaticMarkup(
      createElement(DateTimePicker, {
        value: '2026-11-07T23:45',
        label: 'From time (local)',
        onChange: () => {},
      }),
    );
    expect(html).toContain('aria-label="From time (local)"');
    expect(html).toContain('23:45');
    expect(html).not.toContain('datetime-local');
  });
});
