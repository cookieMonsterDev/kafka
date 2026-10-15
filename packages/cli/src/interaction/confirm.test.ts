import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../errors/aborted-error';
import { createFakeStdin, type FakeStdin } from '../testing/create-fake-stdin';
import { confirmDestructive, requireForce, type ConfirmRuntime } from './confirm';

function createRuntime(overrides: { isTty?: boolean; env?: ConfirmRuntime['env']; stdin?: FakeStdin }): {
  runtime: ConfirmRuntime;
  stdin: FakeStdin;
  stderrWrite: ReturnType<typeof vi.fn>;
} {
  const stdin = overrides.stdin ?? createFakeStdin({ isTTY: overrides.isTty ?? true });
  const stderrWrite = vi.fn((_chunk: string) => true);
  const runtime: ConfirmRuntime = {
    env: overrides.env ?? {},
    stdin,
    stderr: { write: stderrWrite },
  };
  return { runtime, stdin, stderrWrite };
}

describe('confirmDestructive', () => {
  it('resolves immediately when --yes is given, even off a TTY', async () => {
    const { runtime } = createRuntime({ isTty: false });
    await expect(confirmDestructive({ runtime, yes: true, message: 'Delete "orders"?' })).resolves.toBeUndefined();
  });

  it('resolves immediately when cli.confirmDestructive is false, even off a TTY', async () => {
    const { runtime } = createRuntime({ isTty: false });
    await expect(
      confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?', confirmDestructive: false }),
    ).resolves.toBeUndefined();
  });

  it('throws naming --yes when off a TTY without --yes', async () => {
    const { runtime } = createRuntime({ isTty: false });
    await expect(confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' })).rejects.toThrow(
      CliAbortedError,
    );
    await expect(confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' })).rejects.toThrow(/--yes/);
  });

  it('includes the message in the aborted error when off a TTY', async () => {
    const { runtime } = createRuntime({ isTty: false });
    await expect(confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' })).rejects.toThrow(
      /Delete "orders"\?/,
    );
  });

  it('throws naming --yes under CI=true even when isTty is true', async () => {
    const { runtime } = createRuntime({ isTty: true, env: { CI: 'true' } });
    await expect(confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' })).rejects.toThrow(/--yes/);
  });

  it('prompts on a TTY and resolves on "y"', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('y\n');
    await expect(promise).resolves.toBeUndefined();
  });

  it('prompts on a TTY and resolves on "yes" (case-insensitive)', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('YES\n');
    await expect(promise).resolves.toBeUndefined();
  });

  it('prompts on a TTY and aborts on "n"', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('n\n');
    await expect(promise).rejects.toThrow(CliAbortedError);
  });

  it('prompts on a TTY and aborts on a blank answer', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('\n');
    await expect(promise).rejects.toThrow(CliAbortedError);
  });

  it('accumulates data chunks that split before the newline', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('y');
    stdin.emitData('e');
    stdin.emitData('s\n');
    await expect(promise).resolves.toBeUndefined();
  });

  it('resolves from an "end" event when the input has no trailing newline', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('y');
    stdin.emitEnd();
    await expect(promise).resolves.toBeUndefined();
  });

  it('writes the prompt to stderr, not stdout', async () => {
    const { runtime, stdin, stderrWrite } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('y\n');
    await promise;
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('Delete "orders"?'));
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('[y/N]'));
  });

  it('requires --yes when stdout is a TTY but stdin is piped, e.g. `yes | kafka topic delete`', async () => {
    const stdin = createFakeStdin({ isTTY: false });
    // A stdout-TTY flag must not count as interactive: only a stdin TTY can answer the prompt.
    const runtime = { isTty: true, env: {}, stdin, stderr: { write: () => true } };
    await expect(confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' })).rejects.toThrow(/--yes/);
  });

  it('aborts instead of hanging when stdin has already ended', async () => {
    const { runtime } = createRuntime({ stdin: createFakeStdin({ isTTY: true, readableEnded: true }) });
    await expect(confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' })).rejects.toThrow(
      CliAbortedError,
    );
  });

  it('detaches its stdin listeners and pauses stdin once answered', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    const promise = confirmDestructive({ runtime, yes: false, message: 'Delete "orders"?' });
    stdin.emitData('y\n');
    await promise;
    expect(stdin.listenerCount()).toBe(0);
    expect(stdin.pause).toHaveBeenCalled();
  });

  it('never touches stdin when --yes short-circuits the prompt', async () => {
    const { runtime, stdin } = createRuntime({ isTty: true });
    await confirmDestructive({ runtime, yes: true, message: 'Delete "orders"?' });
    expect(stdin.on).not.toHaveBeenCalled();
  });
});

describe('requireForce', () => {
  it('does nothing when force is true', () => {
    expect(() => requireForce({ force: true, reason: 'deleting 12 topics in one call' })).not.toThrow();
  });

  it('throws CliAbortedError naming --force and the reason when force is false', () => {
    expect(() => requireForce({ force: false, reason: 'deleting an internal topic' })).toThrow(CliAbortedError);
    expect(() => requireForce({ force: false, reason: 'deleting an internal topic' })).toThrow(
      /deleting an internal topic requires --force/,
    );
  });

  it('is never waived by cli.confirmDestructive — it takes no such option', () => {
    // requireForce's signature has no confirmDestructive/yes parameter at all: a caller cannot
    // accidentally thread the confirmation-tier override into the force-tier check.
    expect(() => requireForce({ force: false, reason: 'deleting more than 10 topics' })).toThrow(CliAbortedError);
  });
});
