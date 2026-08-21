## Summary

<!-- What changed and why. Link the issue: Closes #123 -->

## Test plan

- [ ] Unit tests (`pnpm test`) cover the change
- [ ] Integration tests run if this touches brokers, protocol, SASL, or admin (`KAFKA_VERSION=… pnpm --filter @cookiemonsterdev/kafka-core test:integration`)
- [ ] Docs or README updated when the public API or workflow changes
- [ ] Docs UI changes keep keyboard access, accessible names, visible focus, contrast, and `prefers-reduced-motion` (see [CONTRIBUTING.md](https://github.com/cookieMonsterDev/kafka/blob/develop/CONTRIBUTING.md#accessibility))

## Checklist

- [ ] Branch name follows `type/short-kebab-description` (see [CONTRIBUTING.md](https://github.com/cookieMonsterDev/kafka/blob/develop/CONTRIBUTING.md))
- [ ] Commits follow Conventional Commits (`feat`, `fix`, `docs`, …)
- [ ] `pnpm lint`, `pnpm format:check`, and `pnpm typecheck` are clean
- [ ] No unrelated refactors or formatting-only noise
