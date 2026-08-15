export function namespace<Namespace extends string>(ns: Namespace) {
  return <Type extends string>(type: Type): `${Namespace}.${Type}` => `${ns}.${type}`;
}
