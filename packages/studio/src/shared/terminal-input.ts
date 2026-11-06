export interface TerminalToken {
  readonly value: string;
  readonly start: number;
  readonly end: number;
}

export function tokenizeTerminalInput(line: string, strict: boolean): TerminalToken[] {
  const tokens: TerminalToken[] = [];
  let value = '';
  let start = -1;
  let quote: string | undefined;
  let escaped = false;
  for (let index = 0; index < line.length; index++) {
    const char = line[index] ?? '';
    if (start < 0 && /\s/.test(char)) continue;
    if (start < 0) start = index;
    if (escaped) {
      value += char;
      escaped = false;
    } else if (char === '\\' && quote !== "'") {
      escaped = true;
    } else if (quote !== undefined) {
      if (char === quote) quote = undefined;
      else value += char;
    } else if (char === '"' || char === "'") {
      quote = char;
    } else if (/\s/.test(char)) {
      tokens.push({ value, start, end: index });
      value = '';
      start = -1;
    } else {
      value += char;
    }
  }
  if (strict && (quote !== undefined || escaped))
    throw new Error('Close the quoted value or finish the escape before running.');
  if (start >= 0) tokens.push({ value, start, end: line.length });
  return tokens.map((token) => {
    const raw = line.slice(token.start, token.end);
    if (raw.startsWith('"') && raw.endsWith('"')) {
      try {
        const decoded: unknown = JSON.parse(raw);
        if (typeof decoded === 'string') return { ...token, value: decoded };
      } catch {
        // Non-JSON shell-style quotes still use the tokenizer's escape rules.
      }
    }
    return token;
  });
}

export function terminalArgv(line: string): string[] {
  const words = tokenizeTerminalInput(line, true).map((token) => token.value);
  if (words[0] === 'kafka') words.shift();
  return words;
}
