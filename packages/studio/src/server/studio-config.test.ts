import { describe, expect, it, vi } from 'vitest';
import { readStudioSection } from './studio-config';

describe('readStudioSection', () => {
  it('returns an empty object when there is no "studio" section', () => {
    expect(readStudioSection(null)).toEqual({});
    expect(readStudioSection({})).toEqual({});
  });

  it('reads every known key', () => {
    const config = readStudioSection({
      studio: { port: 5757, host: '0.0.0.0', openBrowser: false, readOnly: true, maxTail: 500 },
    });
    expect(config).toEqual({ port: 5757, host: '0.0.0.0', openBrowser: false, readOnly: true, maxTail: 500 });
  });

  it('warns and ignores an unknown key', () => {
    const warn = vi.fn();
    const config = readStudioSection({ studio: { bogus: true } }, warn);
    expect(config).toEqual({});
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('unknown "studio.bogus"'));
  });

  it('warns and ignores a malformed "studio" section', () => {
    const warn = vi.fn();
    expect(readStudioSection({ studio: 'nope' }, warn)).toEqual({});
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('must be an object'));
  });

  it.each([
    ['port', 0],
    ['port', 70_000],
    ['port', 'nope'],
  ])('warns and ignores an invalid %s value %j', (key, value) => {
    const warn = vi.fn();
    const config = readStudioSection({ studio: { [key]: value } }, warn);
    expect(config).toEqual({});
    expect(warn).toHaveBeenCalledOnce();
  });

  it('warns and ignores a non-positive maxTail', () => {
    const warn = vi.fn();
    expect(readStudioSection({ studio: { maxTail: 0 } }, warn)).toEqual({});
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('"studio.maxTail"'));
  });

  it('warns and ignores a non-boolean readOnly', () => {
    const warn = vi.fn();
    expect(readStudioSection({ studio: { readOnly: 'yes' } }, warn)).toEqual({});
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('"studio.readOnly"'));
  });

  it('does nothing when warn is omitted', () => {
    expect(readStudioSection({ studio: { bogus: true } })).toEqual({});
  });
});
