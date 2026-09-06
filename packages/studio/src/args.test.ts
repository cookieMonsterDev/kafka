import { describe, expect, it } from 'vitest';
import { StudioUsageError, parseStudioArgs, usageText } from './args';

describe('parseStudioArgs', () => {
  it('defaults to no port/host/browser override and read-only off', () => {
    expect(parseStudioArgs([])).toEqual({ readOnly: false, help: false, version: false });
  });

  it('parses --port', () => {
    expect(parseStudioArgs(['--port', '5757'])).toMatchObject({ port: 5757 });
    expect(parseStudioArgs(['-p', '6000'])).toMatchObject({ port: 6000 });
  });

  it('rejects a non-numeric or out-of-range --port', () => {
    expect(() => parseStudioArgs(['--port', 'abc'])).toThrow(StudioUsageError);
    expect(() => parseStudioArgs(['--port', '0'])).toThrow(StudioUsageError);
    expect(() => parseStudioArgs(['--port', '99999'])).toThrow(StudioUsageError);
    expect(() => parseStudioArgs(['--port', '80.5'])).toThrow(StudioUsageError);
  });

  it('parses --host', () => {
    expect(parseStudioArgs(['--host', '0.0.0.0'])).toMatchObject({ host: '0.0.0.0' });
  });

  it('parses --browser', () => {
    expect(parseStudioArgs(['--browser', 'firefox'])).toMatchObject({ browser: 'firefox' });
  });

  it('treats --no-browser as --browser none', () => {
    expect(parseStudioArgs(['--no-browser'])).toMatchObject({ browser: 'none' });
  });

  it('lets an explicit --browser win when both are given', () => {
    expect(parseStudioArgs(['--no-browser', '--browser', 'firefox'])).toMatchObject({ browser: 'firefox' });
  });

  it('parses --read-only, --help, --version', () => {
    expect(parseStudioArgs(['--read-only'])).toMatchObject({ readOnly: true });
    expect(parseStudioArgs(['--help'])).toMatchObject({ help: true });
    expect(parseStudioArgs(['-h'])).toMatchObject({ help: true });
    expect(parseStudioArgs(['--version'])).toMatchObject({ version: true });
    expect(parseStudioArgs(['-v'])).toMatchObject({ version: true });
  });

  it('rejects an unknown flag', () => {
    expect(() => parseStudioArgs(['--bogus'])).toThrow(StudioUsageError);
  });

  it('rejects a positional argument', () => {
    expect(() => parseStudioArgs(['extra'])).toThrow(StudioUsageError);
  });
});

describe('usageText', () => {
  it('documents every flag', () => {
    const text = usageText();
    for (const flag of ['--port', '--host', '--browser', '--no-browser', '--read-only', '--help', '--version']) {
      expect(text).toContain(flag);
    }
  });
});
