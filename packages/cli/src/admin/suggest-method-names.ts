/** Cheap case-insensitive substring match — enough to suggest a likely typo, not a full fuzzy search. */
export function suggestMethodNames(input: string, candidates: readonly string[], limit = 3): string[] {
  const needle = input.toLowerCase();
  return candidates
    .filter((name) => name.toLowerCase().includes(needle) || needle.includes(name.toLowerCase()))
    .slice(0, limit);
}
