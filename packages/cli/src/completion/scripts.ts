export type CompletionShell = 'bash' | 'zsh' | 'fish';

export const COMPLETION_SHELLS: readonly CompletionShell[] = ['bash', 'zsh', 'fish'];

/**
 * Every script below hands the shell's own notion of "every word typed so far, including the
 * partial one under the cursor" straight to `kafka complete --`, unchanged — the contract
 * `commands/meta/complete.ts` and `completion/complete.ts` are built against. None of the three
 * shells' native completion primitives agree on how that word list is exposed (`COMP_WORDS` vs
 * `$words`/`$CURRENT` vs `commandline`), so each function below adapts its shell's primitive to
 * that one shape rather than teaching `kafka complete` three different calling conventions.
 */

function renderBashScript(programName: string): string {
  return `# kafka shell completion for bash
# Install: eval "$(${programName} completion bash)"
# Or persist it: ${programName} completion bash > /usr/local/etc/bash_completion.d/${programName}

_${programName}_completions() {
  local words
  words=("\${COMP_WORDS[@]:1:COMP_CWORD}")
  local IFS=$'\\n'
  COMPREPLY=($(${programName} complete -- "\${words[@]}"))
}

complete -F _${programName}_completions ${programName}
`;
}

function renderZshScript(programName: string): string {
  return `#compdef ${programName}
# kafka shell completion for zsh
# Install: eval "$(${programName} completion zsh)"
# Or persist it as a file named _${programName} somewhere on your $fpath.

_${programName}_completions() {
  local -a words
  words=("\${(@)words[2,CURRENT]}")
  local IFS=$'\\n'
  local -a completions
  completions=("\${(@f)$(${programName} complete -- "\${words[@]}")}")
  compadd -a completions
}

compdef _${programName}_completions ${programName}
`;
}

function renderFishScript(programName: string): string {
  return `# kafka shell completion for fish
# Install: ${programName} completion fish | source
# Or persist it: ${programName} completion fish > ~/.config/fish/completions/${programName}.fish

function __${programName}_completions
    set -l tokens (commandline -poc)
    set -l current (commandline -ct)
    ${programName} complete -- $tokens[2..-1] $current
end

complete -c ${programName} -f -a '(__${programName}_completions)'
`;
}

const RENDERERS: Readonly<Record<CompletionShell, (programName: string) => string>> = {
  bash: renderBashScript,
  zsh: renderZshScript,
  fish: renderFishScript,
};

export function isCompletionShell(value: string): value is CompletionShell {
  return (COMPLETION_SHELLS as readonly string[]).includes(value);
}

export function renderCompletionScript(shell: CompletionShell, programName: string): string {
  return RENDERERS[shell](programName);
}
