/** A minimal column-aligned table for human output — no box-drawing glyphs, so it degrades cleanly. */
export function renderTable(headers: readonly string[], rows: readonly (readonly string[])[]): string {
  const widths = headers.map((header, i) => {
    let width = header.length;
    for (const row of rows) {
      const length = (row[i] ?? '').length;
      if (length > width) width = length;
    }
    return width;
  });

  const renderRow = (cells: readonly string[]): string =>
    cells
      .map((cell, i) => cell.padEnd(widths[i] ?? 0))
      .join('  ')
      .trimEnd();

  return [renderRow(headers), ...rows.map(renderRow)].join('\n');
}
