export interface DeleteGroupFailure {
  readonly groupId: string;
  readonly errorCode: number;
  readonly error?: { readonly message?: string };
}

/**
 * Matched by `.name`, never `instanceof` — a config file's core might not be the same installed
 * copy as the CLI's own, so a class identity check could silently miss it. `deleteGroups`/
 * `deleteShareGroups` either resolve with every group's result on full success, or throw
 * `KafkaDeleteGroupsError` — whose `.groups` names only the groups that failed — on any failure.
 */
export function isKafkaDeleteGroupsError(
  error: unknown,
): error is { readonly name: string; readonly groups: readonly DeleteGroupFailure[] } {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { name?: unknown }).name === 'KafkaDeleteGroupsError' &&
    Array.isArray((error as { groups?: unknown }).groups)
  );
}
