import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it, vi } from 'vitest';
import type { MessageRecord } from '../../../shared/contracts/message';
import { MessagesTable } from './table';

vi.mock('@tanstack/react-virtual', () => ({
  useVirtualizer: () => ({
    getTotalSize: () => 36,
    getVirtualItems: () => [{ index: 0, start: 0 }],
    measureElement: () => undefined,
  }),
}));

const message: MessageRecord = {
  partition: 2,
  offset: '42',
  timestamp: '1700000000000',
  key: null,
  value: null,
  headers: {},
  size: 0,
};

function render(selectedKey: string | null, messages: readonly MessageRecord[] = [message]): string {
  return renderToStaticMarkup(
    createElement(MessagesTable, {
      mode: 'history',
      filtered: messages,
      decoder: 'utf8',
      selectedKey,
      onSelectKey: () => undefined,
      droppedCount: 0,
    }),
  );
}

describe('MessagesTable accessibility', () => {
  it('keeps column headers and virtual rows in one named table with logical row positions', () => {
    const html = render(null);
    expect(html.match(/<table\b/g)).toHaveLength(1);
    expect(html).toMatch(/<table[^>]*aria-label="Messages"[^>]*aria-rowcount="2"/);
    expect(html).toMatch(/<th[^>]*scope="col"[^>]*>Offset<\/th>/);
    expect(html).toMatch(/<tr[^>]*aria-rowindex="2"/);
    expect(html.indexOf('<thead')).toBeLessThan(html.indexOf('<tbody'));
    expect(html.indexOf('</table>')).toBeGreaterThan(html.indexOf('</tbody>'));
  });

  it('exposes selection through a named native toggle button', () => {
    expect(render('2:42')).toMatch(
      /<button[^>]*type="button"[^>]*aria-label="Show message at partition 2, offset 42"[^>]*aria-pressed="true"/,
    );
    expect(render(null)).toContain('aria-pressed="false"');
    expect(render(null)).not.toContain('aria-selected');
  });

  it('retains the empty state without inventing virtual records', () => {
    const html = render(null, []);
    expect(html).toContain('No messages found');
    expect(html).toContain('aria-rowcount="1"');
    expect(html).not.toContain('aria-pressed');
  });
});
