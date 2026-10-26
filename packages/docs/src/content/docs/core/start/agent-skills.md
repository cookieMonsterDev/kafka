---
title: Agent skills
description: Coding-agent skills that ship inside the npm packages and update with them
order: 4
section: start
---

`@cookiemonsterdev/kafka-core`, `@cookiemonsterdev/kafka-cli`, `@cookiemonsterdev/kafka-studio`,
and `@cookiemonsterdev/kafka-config` ship [Agent Skills](https://agentskills.io) in a
`skills/` folder inside each npm package. Coding agents such as Claude Code, Cursor, and Codex
can read them to set up, use, and migrate to these packages without guessing the API. Each skill
matches the package version you installed, so upgrading the package also updates its skills.

## Use them in your project

After installing the packages, run [TanStack Intent](https://tanstack.com/intent/latest), which
finds skills in `node_modules` and adds task-to-skill mappings to your agent config
(`AGENTS.md` by default):

```sh
npx @tanstack/intent@latest install
```

To see what is available, or to read a skill directly:

```sh
npx @tanstack/intent@latest list
npx @tanstack/intent@latest load @cookiemonsterdev/kafka-core#kafka-core-consumer
```

You can also point your agent at a skill file directly, for example
`node_modules/@cookiemonsterdev/kafka-core/skills/kafka-core/SKILL.md`.

## Available skills

| Package                          | Skill                             | Use it for                                                 |
| -------------------------------- | --------------------------------- | ---------------------------------------------------------- |
| `@cookiemonsterdev/kafka-core`   | `kafka-core`                      | Entry point: shared rules and routing to the skills below  |
| `@cookiemonsterdev/kafka-core`   | `kafka-core-setup`                | Install, connect, TLS/SASL, config files, env variables    |
| `@cookiemonsterdev/kafka-core`   | `kafka-core-producer`             | Sending, batching, compression, transactions               |
| `@cookiemonsterdev/kafka-core`   | `kafka-core-consumer`             | Groups, commits, rebalances, assign mode, share groups     |
| `@cookiemonsterdev/kafka-core`   | `kafka-core-admin`                | Topics, configs, group offsets, ACLs, tokens, KRaft        |
| `@cookiemonsterdev/kafka-core`   | `kafka-core-migrate-from-kafkajs` | Replacing kafkajs                                          |
| `@cookiemonsterdev/kafka-cli`    | `kafka-cli`                       | Terminal and script administration with the `kafka` binary |
| `@cookiemonsterdev/kafka-studio` | `kafka-studio`                    | Launching the local web UI safely                          |
| `@cookiemonsterdev/kafka-config` | `kafka-config`                    | Loading your own config files; config files that fail      |

Skills are written from this documentation and checked in CI. If a skill gives wrong guidance, please
[open an issue](https://github.com/cookieMonsterDev/kafka/issues).
