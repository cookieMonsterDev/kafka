export interface ColorDecisionInput {
  readonly isTty: boolean;
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly colorFlag: boolean;
  readonly noColorFlag: boolean;
}

/** `--color`/`--no-color` beat `NO_COLOR`/`FORCE_COLOR`, which beat whether stdout is a TTY. */
export function shouldUseColor(input: ColorDecisionInput): boolean {
  if (input.colorFlag) return true;
  if (input.noColorFlag) return false;
  if (input.env.NO_COLOR !== undefined) return false;
  if (input.env.FORCE_COLOR !== undefined && input.env.FORCE_COLOR !== '0') return true;
  return input.isTty;
}

export interface Palette {
  bold(text: string): string;
  dim(text: string): string;
  red(text: string): string;
  green(text: string): string;
  yellow(text: string): string;
}

const CODES: Record<keyof Palette, readonly [number, number]> = {
  bold: [1, 22],
  dim: [2, 22],
  red: [31, 39],
  green: [32, 39],
  yellow: [33, 39],
};

function wrap([open, close]: readonly [number, number]): (text: string) => string {
  return (text) => `[${String(open)}m${text}[${String(close)}m`;
}

function identity(text: string): string {
  return text;
}

export function createPalette(enabled: boolean): Palette {
  if (!enabled) {
    return { bold: identity, dim: identity, red: identity, green: identity, yellow: identity };
  }
  return {
    bold: wrap(CODES.bold),
    dim: wrap(CODES.dim),
    red: wrap(CODES.red),
    green: wrap(CODES.green),
    yellow: wrap(CODES.yellow),
  };
}
