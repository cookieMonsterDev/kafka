/** Raised when an operation refuses to run without explicit confirmation it did not get. */
export class CliAbortedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CliAbortedError';
  }
}
