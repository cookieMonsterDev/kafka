/** Reads a displayable message off whatever a query or mutation rejected with. */
export function errorMessage(error: unknown): string | undefined {
  if (error instanceof Error && error.message !== '') return error.message;
  if (typeof error === 'string' && error !== '') return error;
  return undefined;
}
