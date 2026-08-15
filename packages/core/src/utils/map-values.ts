export function mapValues<T extends Record<string, unknown>, R>(
  obj: T,
  mapper: (value: T[keyof T], key: string) => R,
): { [K in keyof T]: R } {
  const result = {} as { [K in keyof T]: R };

  for (const [key, value] of Object.entries(obj) as Array<[keyof T, T[keyof T]]>) {
    result[key] = mapper(value, key as string);
  }

  return result;
}
