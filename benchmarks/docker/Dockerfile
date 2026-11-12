# syntax=docker/dockerfile:1.7
#
# Benchmark client images. runner/images.ts assembles the build context and passes every argument:
#
#   core-src/   the repository at the kafka-core commit under test (git archive), without benchmarks/
#   bench/      benchmarks/{package.json,pnpm-lock.yaml,pnpm-workspace.yaml,tsconfig.json,client,runner}
#
# Base image arguments arrive as digest-pinned references (`node:24-trixie-slim@sha256:...`).
#
# Stages:
#   core-builder   builds and packs kafka-config and kafka-core from core-src, installs the tarballs
#   deps           competitor clients from the committed benchmark lockfile, built for DEPS_IMAGE's Node ABI
#   bench-modules  deps plus the kafka-core tree from core-builder
#   runtime-node   final image for Node (RUNTIME_IMAGE = a Node image)
#   runtime-bun    final image for Bun (RUNTIME_IMAGE = an oven/bun image, DEPS_IMAGE = the Node image whose ABI Bun reports)
#   typecheck      tsc --noEmit over client/ and runner/ against the installed clients; exports nothing

ARG CORE_BUILDER_IMAGE=node:26-trixie-slim
ARG DEPS_IMAGE=node:24-trixie-slim
ARG RUNTIME_IMAGE=node:24-trixie-slim
# Matches packageManager in the repository and in benchmarks/package.json. Node 25+ no longer ships
# corepack, so pnpm is installed from npm instead.
ARG PNPM_VERSION=12.15.0

# ---------------------------------------------------------------------------------------------------
FROM ${CORE_BUILDER_IMAGE} AS core-builder
ARG PNPM_VERSION
ENV CI=true
RUN npm install --global --no-audit --no-fund "pnpm@${PNPM_VERSION}"

WORKDIR /src
COPY core-src/ ./
# The commit is part of the cache key of every following step, so this stage rebuilds exactly when the
# kafka-core commit (or, for a worktree build, the copied content) changes.
ARG CORE_SHA
RUN test -n "${CORE_SHA}" && echo "${CORE_SHA}" > /core-sha

RUN --mount=type=cache,id=kafka-bench-pnpm-store,target=/pnpm-store,sharing=locked \
    pnpm install --frozen-lockfile --store-dir /pnpm-store --filter '@cookiemonsterdev/kafka-core...'
RUN pnpm --filter '@cookiemonsterdev/kafka-core...' run build

RUN mkdir -p /out/core-install \
 && (cd packages/config && pnpm pack --pack-destination /out) \
 && (cd packages/core && pnpm pack --pack-destination /out) \
 && mv /out/cookiemonsterdev-kafka-config-*.tgz /out/kafka-config.tgz \
 && mv /out/cookiemonsterdev-kafka-core-*.tgz /out/kafka-core.tgz

# Install the two tarballs the way a consumer would, but outside the benchmark lockfile (plan 7.4): the
# lockfile then never depends on a kafka-core commit. Both tarballs are installed together so core's
# `@cookiemonsterdev/kafka-config` dependency is satisfied by the config built from the same commit,
# not by a published release. Core's other runtime dependencies are pinned through npm overrides to
# the versions in the repository lockfile at that commit, so the install stays reproducible.
RUN pnpm --filter @cookiemonsterdev/kafka-core list --prod --depth Infinity --json > /tmp/core-dependencies.json
RUN node --input-type=module <<'EOF'
import { readFileSync, writeFileSync } from 'node:fs';

const [project] = JSON.parse(readFileSync('/tmp/core-dependencies.json', 'utf8'));
const pins = new Map();
const visit = (dependencies) => {
  for (const [name, dependency] of Object.entries(dependencies ?? {})) {
    visit(dependency.dependencies);
    if (name.startsWith('@cookiemonsterdev/') || String(dependency.version).startsWith('link:')) continue;
    const pinned = pins.get(name);
    if (pinned !== undefined && pinned !== dependency.version) {
      throw new Error(`${name} resolves to both ${pinned} and ${dependency.version}; a flat override cannot pin it`);
    }
    pins.set(name, dependency.version);
  }
};
visit(project.dependencies);

const manifest = {
  name: 'kafka-bench-core-install',
  private: true,
  dependencies: {
    '@cookiemonsterdev/kafka-config': 'file:../kafka-config.tgz',
    '@cookiemonsterdev/kafka-core': 'file:../kafka-core.tgz',
  },
  overrides: Object.fromEntries([...pins].sort(([a], [b]) => a.localeCompare(b))),
};
writeFileSync('/out/core-install/package.json', JSON.stringify(manifest, null, 2) + '\n');
EOF
WORKDIR /out/core-install
RUN --mount=type=cache,id=kafka-bench-npm-cache,target=/root/.npm,sharing=locked \
    npm install --omit=dev --no-package-lock --ignore-scripts --no-audit --no-fund \
 && rm -f node_modules/.package-lock.json \
 && test ! -e node_modules/@cookiemonsterdev/kafka-core/node_modules/@cookiemonsterdev/kafka-config

# ---------------------------------------------------------------------------------------------------
# Toolchain and pnpm on the Node image whose ABI the dependencies are installed for.
FROM ${DEPS_IMAGE} AS deps-base
ARG PNPM_VERSION
ENV CI=true
# The Confluent client falls back to compiling its addon (node-gyp, librdkafka) when no prebuilt binary
# matches the Node ABI, as on Node 26. The toolchain lets that attempt run; it never reaches a runtime image.
# librdkafka's configure downloads and statically builds zlib, OpenSSL, zstd and curl, hence curl, perl and patch.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends python3 make g++ curl ca-certificates perl patch \
 && rm -rf /var/lib/apt/lists/*
RUN npm install --global --no-audit --no-fund "pnpm@${PNPM_VERSION}"
WORKDIR /bench
COPY bench/package.json bench/pnpm-lock.yaml bench/pnpm-workspace.yaml ./

FROM deps-base AS deps
# Confluent is an optionalDependency: when its addon cannot be downloaded or built, pnpm reports the
# failure and skips the package instead of failing the install, and the probe records the cell as unsupported.
# Known outcome for 1.11.0 on Node 26: the source build compiles librdkafka, then fails when it merges the
# static libraries with an `ar -M` script, because GNU ar's MRI parser cuts paths at `@` and every install
# location (`node_modules/@confluentinc/...`) contains one. A consumer install fails the same way.
# Own cache id: this is the only stage that runs a third-party install script (as root, with network), so it
# must not share a writable store with the stages that build kafka-core.
RUN --mount=type=cache,id=kafka-bench-pnpm-store-deps,target=/pnpm-store,sharing=locked \
    pnpm install --frozen-lockfile --prod --store-dir /pnpm-store

# Adds kafka-core's npm-installed tree next to pnpm's top-level entries. A name present in both would be
# silently replaced by a plain copy, so any overlap fails the build instead.
FROM deps AS bench-modules
COPY --from=core-builder /out/core-install/node_modules/ /tmp/core-modules/
RUN cd /tmp/core-modules \
 && for entry in * @*/*; do \
      [ -e "$entry" ] || continue; \
      case "$entry" in @*/*) ;; @*) continue ;; esac; \
      if [ -e "/bench/node_modules/$entry" ]; then echo "kafka-core dependency $entry collides with a competitor dependency" >&2; exit 1; fi; \
    done \
 && cp -a /tmp/core-modules/. /bench/node_modules/ \
 && rm -rf /tmp/core-modules

# ---------------------------------------------------------------------------------------------------
FROM deps-base AS typecheck
# Types only: no addon is loaded, so install scripts are skipped.
RUN --mount=type=cache,id=kafka-bench-pnpm-store,target=/pnpm-store,sharing=locked \
    pnpm install --frozen-lockfile --ignore-scripts --store-dir /pnpm-store
COPY --from=core-builder /out/core-install/node_modules/ ./node_modules/
COPY bench/tsconfig.json ./
COPY bench/client ./client
COPY bench/runner ./runner
RUN ./node_modules/.bin/tsc --noEmit -p tsconfig.json

# ---------------------------------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime-base
WORKDIR /bench
# Ordered from least to most frequently changing. pnpm never runs again on this tree, so it cannot prune
# the kafka-core packages that were added next to its own.
COPY --from=bench-modules /bench/node_modules ./node_modules
COPY --from=core-builder /core-sha ./core-sha
# package.json carries "type": "module", which the runtimes need to treat client/*.ts as ES modules.
COPY bench/package.json ./
COPY bench/client ./client

FROM runtime-base AS runtime-node
USER node
ENTRYPOINT ["node", "client/main.ts"]

FROM runtime-base AS runtime-bun
USER bun
ENTRYPOINT ["bun", "client/main.ts"]
