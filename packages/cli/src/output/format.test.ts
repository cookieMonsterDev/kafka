import { describe, expect, it, vi } from 'vitest';
import { CLI_LOG_LEVELS } from './logger';
import { createCommandOutput, resolveOutputFormat, writeCliError, writeError, writeFormatted } from './format';

describe('resolveOutputFormat', () => {
  it('defaults to human', () => {
    expect(resolveOutputFormat({ jsonFlag: false, env: {} })).toBe('human');
  });

  it('--json wins over everything', () => {
    expect(resolveOutputFormat({ jsonFlag: true, formatFlag: 'human', env: { KAFKA_OUTPUT: 'human' } })).toBe('json');
  });

  it('--format wins over KAFKA_OUTPUT', () => {
    expect(resolveOutputFormat({ jsonFlag: false, formatFlag: 'human', env: { KAFKA_OUTPUT: 'json' } })).toBe('human');
  });

  it('KAFKA_OUTPUT=json is honored when nothing else is set', () => {
    expect(resolveOutputFormat({ jsonFlag: false, env: { KAFKA_OUTPUT: 'json' } })).toBe('json');
  });
});

describe('writeFormatted', () => {
  it('writes only the human rendering in human mode', () => {
    const write = vi.fn((_chunk: string) => true);
    const jsonRender = vi.fn(() => '{}');
    writeFormatted({ write }, 'human', { human: () => 'a table', json: jsonRender });

    expect(write).toHaveBeenCalledWith('a table\n');
    expect(jsonRender).not.toHaveBeenCalled();
  });

  it('writes only the json rendering in json mode', () => {
    const write = vi.fn((_chunk: string) => true);
    const humanRender = vi.fn(() => 'a table');
    writeFormatted({ write }, 'json', { human: humanRender, json: () => '{"a":1}' });

    expect(write).toHaveBeenCalledWith('{"a":1}\n');
    expect(humanRender).not.toHaveBeenCalled();
  });
});

describe('writeError', () => {
  it('writes to stderr in human mode', () => {
    const stdout = vi.fn((_chunk: string) => true);
    const stderr = vi.fn((_chunk: string) => true);
    writeError({ stdout: { write: stdout }, stderr: { write: stderr } }, 'human', 'boom');

    expect(stderr).toHaveBeenCalledWith('kafka: boom\n');
    expect(stdout).not.toHaveBeenCalled();
  });

  it('writes a JSON envelope to stdout in json mode, never to stderr', () => {
    const stdout = vi.fn((_chunk: string) => true);
    const stderr = vi.fn((_chunk: string) => true);
    writeError({ stdout: { write: stdout }, stderr: { write: stderr } }, 'json', 'boom');

    expect(stderr).not.toHaveBeenCalled();
    expect(stdout).toHaveBeenCalledTimes(1);
    const written = stdout.mock.calls[0]?.[0] ?? '';
    expect(JSON.parse(written)).toEqual({ error: { message: 'boom' } });
  });
});

describe('writeCliError', () => {
  it('writes the message and one indented line per item to stderr in human mode', () => {
    const stdout = vi.fn((_chunk: string) => true);
    const stderr = vi.fn((_chunk: string) => true);
    writeCliError({ stdout: { write: stdout }, stderr: { write: stderr } }, 'human', {
      exitCode: 1,
      message: 'two topics failed',
      items: [{ message: 'orders exists' }, { message: 'payments: timeout' }],
    });

    expect(stdout).not.toHaveBeenCalled();
    expect(stderr).toHaveBeenCalledWith('kafka: two topics failed\n  - orders exists\n  - payments: timeout\n');
  });

  it('writes message and items as JSON on stdout in json mode', () => {
    const stdout = vi.fn((_chunk: string) => true);
    const stderr = vi.fn((_chunk: string) => true);
    writeCliError({ stdout: { write: stdout }, stderr: { write: stderr } }, 'json', {
      exitCode: 1,
      message: 'boom',
      items: [{ message: 'a' }],
    });

    expect(stderr).not.toHaveBeenCalled();
    const written = stdout.mock.calls[0]?.[0] ?? '';
    expect(JSON.parse(written)).toEqual({ error: { message: 'boom', items: [{ message: 'a' }] } });
  });
});

describe('createCommandOutput', () => {
  it('wires the palette, logger, write, and error from the given options', () => {
    const stdout = vi.fn((_chunk: string) => true);
    const stderr = vi.fn((_chunk: string) => true);
    const output = createCommandOutput({
      stdout: { write: stdout },
      stderr: { write: stderr },
      format: 'human',
      useColor: false,
      logLevel: CLI_LOG_LEVELS.WARN,
    });

    output.write({ human: () => 'ok', json: () => '{}' });
    expect(stdout).toHaveBeenCalledWith('ok\n');

    output.error('bad');
    expect(stderr).toHaveBeenCalledWith('kafka: bad\n');

    expect(output.palette.bold('x')).toBe('x');

    output.log.warn('careful');
    expect(stderr).toHaveBeenCalledWith('careful\n');
  });
});
