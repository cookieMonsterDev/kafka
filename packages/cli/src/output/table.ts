/** A minimal column-aligned table for human output — no box-drawing glyphs, so it degrades cleanly. */
export function renderTable(headers: readonly string[], rows: readonly (readonly string[])[]): string {
  const widths = headers.map((header, i) => Math.max(header.length, ...rows.map((row) => (row[i] ?? '').length)));

  const renderRow = (cells: readonly string[]): string =>
    cells
      .map((cell, i) => cell.padEnd(widths[i] ?? 0))
      .join('  ')
      .trimEnd();

  return [renderRow(headers), ...rows.map(renderRow)].join('\n');
}
