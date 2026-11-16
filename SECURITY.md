# Security policy

## Supported versions

Security fixes go to the latest release of each package and ship as a new patch release. Older
versions are not patched; upgrade to the latest release to get a fix.

| Package                          | Supported    |
| -------------------------------- | ------------ |
| `@cookiemonsterdev/kafka-core`   | latest `2.x` |
| `@cookiemonsterdev/kafka-config` | latest `1.x` |
| `@cookiemonsterdev/kafka-cli`    | latest `1.x` |
| `@cookiemonsterdev/kafka-studio` | latest `1.x` |

The standalone `kafka` and `kafka-studio` binaries attached to GitHub releases follow the
version of the package they are built from.

## Reporting a vulnerability

Do not open a public issue, pull request, or discussion for a security problem.

Report it privately through
[GitHub private vulnerability reporting](https://github.com/cookieMonsterDev/kafka/security/advisories/new)
(**Security → Report a vulnerability**). Include as much of the following as you can:

- the affected package and version, or the git commit
- the runtime (Node.js or Bun version) and the operating system
- the broker setup and authentication mechanism involved, if any
- steps or a minimal script to reproduce the problem
- the impact you expect: what an attacker can read, change, or disrupt

## What happens next

This project has one maintainer, so handling is best effort. The aim is to:

1. acknowledge the report within 7 days;
2. confirm whether it is a vulnerability and agree on a severity with you;
3. release a fixed version, then publish a GitHub security advisory that credits you unless you
   prefer to stay anonymous.

Please keep the details private until the advisory is published. Affected versions are not
unpublished from npm; the advisory names the fixed version to upgrade to.

## Scope

In scope: the published packages and standalone binaries listed above, and the code in this
repository that builds and releases them.

Out of scope:

- vulnerabilities in Apache Kafka brokers or other servers the client connects to
- vulnerabilities in a dependency that this project's code does not expose; report those to the
  dependency's maintainers
- behavior that the documentation already marks as unsafe, such as exposing Kafka Studio beyond
  `127.0.0.1` with `--host`
