import { execFileSync } from 'node:child_process';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { COMPLETION_SHELLS, isCompletionShell, renderCompletionScript } from './scripts';

/**
 * Whether `binary` resolves on this machine's `PATH` — the fish syntax check below skips itself
 * if not, since fish (unlike bash/zsh) isn't universally preinstalled.
 */
function hasBinary(binary: string): boolean {
  try {
    execFileSync(binary, ['--version'], { stdio: 'ignore' });
    return true;
  } catch (error) {
    const code = error instanceof Error ? (error as NodeJS.ErrnoException).code : undefined;
    return code !== 'ENOENT';
  }
}

function writeToTempFile(contents: string, filename: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'kafka-completion-'));
  const path = join(dir, filename);
  writeFileSync(path, contents);
  return path;
}

describe('renderCompletionScript', () => {
  it('names kafka complete -- as the completion entry point for every shell', () => {
    for (const shell of COMPLETION_SHELLS) {
      expect(renderCompletionScript(shell, 'kafka')).toContain('kafka complete --');
    }
  });

  it('parametrizes the emitted program name rather than hardcoding it', () => {
    for (const shell of COMPLETION_SHELLS) {
      const script = renderCompletionScript(shell, 'my-kafka');
      expect(script).toContain('my-kafka complete --');
      expect(script).not.toContain(' kafka complete --');
    }
  });

  it.each(['bash', 'zsh'] as const)('is syntactically valid %s', (shell) => {
    const script = renderCompletionScript(shell, 'kafka');
    const path = writeToTempFile(script, `completion.${shell}`);
    expect(() => execFileSync(shell, ['-n', path], { stdio: 'pipe' })).not.toThrow();
  });

  it.skipIf(!hasBinary('fish'))('is syntactically valid fish', () => {
    const script = renderCompletionScript('fish', 'kafka');
    const path = writeToTempFile(script, 'completion.fish');
    expect(() => execFileSync('fish', ['--no-execute', path], { stdio: 'pipe' })).not.toThrow();
  });

  it('zsh: actually forwards the typed words to kafka complete --, not an empty list', () => {
    // A syntax check (`zsh -n`, above) can't catch this: `local -a words` followed by an
    // assignment that reads `$words` shadows the completion system's own special `$words` with a
    // fresh empty array *before* the assignment runs, silently discarding every typed word. This
    // fakes just enough of a zsh completion invocation (the special `$words`/`$CURRENT`
    // parameters, a `kafka` function standing in for the real binary, a `compadd` that prints the
    // array it's given) to exercise the real rendered function and prove it forwards them.
    const script = renderCompletionScript('zsh', 'kafka');
    const harness = `
kafka() { shift 2; printf '%s\\n' "$@"; }
compadd() { eval "print -l -- \\"\\\${\${2}[@]}\\""; }
compdef() { :; }

${script}

words=(kafka topic create)
CURRENT=3
_kafka_completions
`;
    const path = writeToTempFile(harness, 'completion-harness.zsh');
    const output = execFileSync('zsh', [path], { encoding: 'utf8' });
    expect(output.trim().split('\n')).toEqual(['topic', 'create']);
  });
});

describe('isCompletionShell', () => {
  it('accepts every declared shell', () => {
    for (const shell of COMPLETION_SHELLS) {
      expect(isCompletionShell(shell)).toBe(true);
    }
  });

  it('rejects anything else', () => {
    expect(isCompletionShell('powershell')).toBe(false);
    expect(isCompletionShell('')).toBe(false);
  });
});
