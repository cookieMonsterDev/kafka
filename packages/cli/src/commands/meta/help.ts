import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { renderGroupHelp, renderLeafHelp, renderRootHelp, type HelpRenderOptions } from '../../help/render';
import type { Runtime } from '../../runtime';

export function runHelpCommand(
  runtime: Runtime,
  leaves: readonly CommandSpec[],
  groups: ReadonlySet<string>,
  path: readonly string[],
  options: HelpRenderOptions,
): number {
  if (path.length === 0) {
    runtime.stdout.write(`${renderRootHelp(leaves, groups, options)}\n`);
    return EXIT_CODES.ok;
  }

  const key = path.join(' ');
  const leaf = leaves.find((command) => command.path.join(' ') === key);
  if (leaf !== undefined) {
    runtime.stdout.write(`${renderLeafHelp(leaf, options)}\n`);
    return EXIT_CODES.ok;
  }

  if (groups.has(key)) {
    runtime.stdout.write(`${renderGroupHelp(path, leaves, options)}\n`);
    return EXIT_CODES.ok;
  }

  runtime.stderr.write(`kafka: unknown command "${key}"\n`);
  return EXIT_CODES.usage;
}
