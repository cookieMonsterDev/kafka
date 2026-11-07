// @vitest-environment happy-dom
import { createElement, useState } from 'react';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DateTimePicker } from './date-time-picker';
import { SelectInput } from './base-select';
import { ResetOffsetsDialog } from '../groups/reset-dialog';

function DateField() {
  const [value, setValue] = useState('2026-11-07T23:45');
  return createElement(DateTimePicker, { value, onChange: setValue, label: 'From time (local)' });
}
function SelectField() {
  const [value, setValue] = useState('');
  return createElement(SelectInput, {
    value,
    onValueChange: setValue,
    'aria-label': 'Partitioner',
    items: [
      { value: '', label: 'default' },
      { value: 'legacy', label: 'Legacy' },
    ],
  });
}
afterEach(() => cleanup());

describe('picker keyboard and popup interactions', () => {
  it('moves calendar focus with arrows, selects with Enter, and restores trigger focus', async () => {
    const user = userEvent.setup();
    render(createElement(DateField));
    const trigger = screen.getByRole('button', { name: 'From time (local)', description: /23:45/ });
    await user.click(trigger);
    await waitFor(() =>
      expect(document.activeElement?.getAttribute('data-day')).toBe(new Date(2026, 10, 7).toLocaleDateString()),
    );
    await user.keyboard('{ArrowRight}');
    await waitFor(() =>
      expect(document.activeElement?.getAttribute('data-day')).toBe(new Date(2026, 10, 8).toLocaleDateString()),
    );
    await user.keyboard('{Enter}');
    await user.click(screen.getByRole('button', { name: 'Done' }));
    expect(trigger.textContent).toContain('8');
    expect(trigger.textContent).toContain('23:45');
    await waitFor(() => expect(document.activeElement).toBe(trigger));
  });
  it('changes the hour through a shadcn select and clears the value', async () => {
    const user = userEvent.setup();
    render(createElement(DateField));
    await user.click(screen.getByRole('button', { name: 'From time (local)' }));
    await user.click(screen.getByRole('combobox', { name: 'From time (local): hour' }));
    await user.click(screen.getByRole('option', { name: '08' }));
    await user.click(screen.getByRole('button', { name: 'Done' }));
    expect(screen.getByRole('button', { name: 'From time (local)' }).textContent).toContain('08:45');
    await user.click(screen.getByRole('button', { name: 'From time (local)' }));
    await user.click(screen.getByRole('button', { name: 'Clear' }));
    expect(screen.getByRole('button', { name: 'From time (local)' }).textContent).toContain('Pick date and time');
  });
  it('selects a Base UI option while preserving labels and an empty default value', async () => {
    const user = userEvent.setup();
    render(createElement(SelectField));
    const trigger = screen.getByRole('combobox', { name: 'Partitioner' });
    expect(trigger.textContent).toContain('default');
    await user.click(trigger);
    await user.click(screen.getByRole('option', { name: 'Legacy' }));
    expect(trigger.textContent).toContain('Legacy');
  });
  it('keeps Reset offsets open when selecting a date inside its nested popover', async () => {
    const user = userEvent.setup();
    const confirm = vi.fn();
    render(
      createElement(ResetOffsetsDialog, {
        open: true,
        onOpenChange: () => {},
        onConfirm: confirm,
        partitionLag: [{ topic: 'orders', partition: 0, committedOffset: '0', logEndOffset: '10', lag: '10' }],
      }),
    );
    await user.click(screen.getByRole('combobox', { name: 'Reset to' }));
    await user.click(screen.getByRole('option', { name: 'The offset as of a timestamp' }));
    await user.click(screen.getByRole('button', { name: 'Reset offset timestamp' }));
    await waitFor(() => expect(document.activeElement?.hasAttribute('data-day')).toBe(true));
    await user.keyboard('{ArrowRight}{Enter}');
    await user.click(screen.getByRole('button', { name: 'Done' }));
    expect(screen.getByRole('dialog', { name: 'Reset offsets' })).toBeTruthy();
    await user.click(screen.getByRole('button', { name: 'Reset offsets' }));
    expect(confirm).toHaveBeenCalledWith('orders', [{ partition: 0, to: 'timestamp', timestamp: expect.any(Number) }]);
  });
});
