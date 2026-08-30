import { describe, expect, it } from 'vitest';
import { createPalette, shouldUseColor } from './colors';

describe('shouldUseColor', () => {
  it('defaults to whether stdout is a TTY', () => {
    expect(shouldUseColor({ isTty: true, env: {}, colorFlag: false, noColorFlag: false })).toBe(true);
    expect(shouldUseColor({ isTty: false, env: {}, colorFlag: false, noColorFlag: false })).toBe(false);
  });

  it('NO_COLOR disables even on a TTY', () => {
    expect(shouldUseColor({ isTty: true, env: { NO_COLOR: '1' }, colorFlag: false, noColorFlag: false })).toBe(false);
  });

  it('FORCE_COLOR enables even off a TTY, unless it is "0"', () => {
    expect(shouldUseColor({ isTty: false, env: { FORCE_COLOR: '1' }, colorFlag: false, noColorFlag: false })).toBe(
      true,
    );
    expect(shouldUseColor({ isTty: false, env: { FORCE_COLOR: '0' }, colorFlag: false, noColorFlag: false })).toBe(
      false,
    );
  });

  it('--no-color always wins over env and TTY', () => {
    expect(shouldUseColor({ isTty: true, env: { FORCE_COLOR: '1' }, colorFlag: false, noColorFlag: true })).toBe(false);
  });

  it('--color always wins over env and TTY', () => {
    expect(shouldUseColor({ isTty: false, env: { NO_COLOR: '1' }, colorFlag: true, noColorFlag: false })).toBe(true);
  });
});

describe('createPalette', () => {
  it('wraps text in ANSI codes when enabled', () => {
    const palette = createPalette(true);
    expect(palette.bold('x')).not.toBe('x');
    expect(palette.bold('x')).toContain('x');
  });

  it('returns text unchanged when disabled', () => {
    const palette = createPalette(false);
    expect(palette.bold('x')).toBe('x');
    expect(palette.red('x')).toBe('x');
  });
});
