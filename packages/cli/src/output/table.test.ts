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

  it('computes widths for 200,000 rows without throwing, and keeps correct column alignment', () => {
    const rowCount = 200_000;
    const rows: string[][] = Array.from({ length: rowCount }, () => ['x', 'y']);
    const longValue = 'a-much-longer-value-than-the-header';
    rows[42] = [longValue, 'y'];

    let table = '';
    expect(() => {
      table = renderTable(['NAME', 'VALUE'], rows);
    }).not.toThrow();

    const lines = table.split('\n');
    expect(lines).toHaveLength(rowCount + 1);

    const headerStart = lines[0]?.indexOf('VALUE') ?? -1;
    expect(headerStart).toBe(longValue.length + 2);
    const longRowLine = lines[43];
    expect(longRowLine?.indexOf('y')).toBe(headerStart);
  });
});
