---
title: Installation
description: Run @cookiemonsterdev/kafka-cli with npx, pnpm dlx, or a global install
order: 2
section: start
---

Node.js **24** is required. No install is needed to try it once:

```sh
npx @cookiemonsterdev/kafka-cli ping --brokers localhost:9092
```

```sh
pnpm dlx @cookiemonsterdev/kafka-cli topic list --brokers localhost:9092
```

Or install it once, either globally or as a dev dependency of a project that talks to Kafka:

```sh
npm install -g @cookiemonsterdev/kafka-cli
kafka --version
```

```sh
npm install --save-dev @cookiemonsterdev/kafka-cli
```

Shell completion (bash, zsh, fish) is generated on demand — nothing to install separately:

```sh
eval "$(kafka completion bash)"   # or: zsh, fish
```

Next: [Getting started](./getting-started/).
