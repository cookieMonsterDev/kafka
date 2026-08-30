import { describe, expect, it } from 'vitest';
import { renderTable } from './table';

describe('renderTable', () => {
  it('aligns every row so a given column starts at the same character index', () => {
    const table = renderTable(
      ['NAME', 'PARTITIONS'],
      [
        ['orders', '3'],
        ['payments-history', '12'],
      ],
    );
    const lines = table.split('\n');
    expect(lines).toHaveLength(3);

    // Every row's second column must start at the same index as the header's.
    const headerStart = lines[0]?.indexOf('PARTITIONS') ?? -1;
    for (const line of lines) {
      const dataStart = line.search(/\S+$/);
      expect(dataStart).toBe(headerStart);
    }
    expect(headerStart).toBeGreaterThan('payments-history'.length);
  });

  it('renders just the header for no rows', () => {
    expect(renderTable(['NAME'], [])).toBe('NAME');
  });

  it('trims trailing whitespace from every line', () => {
    const table = renderTable(['A', 'B'], [['x', 'y']]);
    for (const line of table.split('\n')) {
      expect(line.endsWith(' ')).toBe(false);
    }
  });
});
