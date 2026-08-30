/** What every error path in the CLI reduces to: an exit code, a message, and optional sub-items. */
export interface CliError {
  readonly exitCode: number;
  readonly message: string;
  readonly items?: readonly { message: string }[];
}
