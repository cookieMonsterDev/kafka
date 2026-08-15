export interface PackageInfo {
  readonly name: string;
  readonly version: string;
}

export const packageInfo: PackageInfo = {
  name: '@kafka/core',
  version: '0.0.0',
};

export function greet(target: string): string {
  return `${packageInfo.name} says hello to ${target}`;
}
