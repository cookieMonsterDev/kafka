---
title: Command reference
description: Every kafka command, one page each, generated from the command registry
order: 1
section: reference
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

## General

- [`completion`](./completion/) — Print a shell completion script for bash, zsh, or fish
- [`doctor`](./doctor/) — Report where connection settings come from — config file, environment, profile, or flag
- [`init`](./init/) — Scaffold a kafka.config file in the current directory
- [`ping`](./ping/) — Check connectivity to the cluster
- [`profiles`](./profiles/) — List the named connection profiles configured under cli.profiles

## acl

- [`acl add`](./acl/add/) — Create one or more ACLs, granting or denying an operation
- [`acl list`](./acl/list/) — List ACLs matching a filter, or every ACL by default
- [`acl remove`](./acl/remove/) — Delete every ACL matching a filter, per principal

## admin

- [`admin call`](./admin/call/) — Call any Admin method by name — the escape hatch for everything without its own command
- [`admin methods`](./admin/methods/) — List every Admin method admin call can reach, and how each is classified

## cluster

- [`cluster elect-leaders`](./cluster/elect-leaders/) — Trigger a preferred or unclean leader election on one or more partitions
- [`cluster features`](./cluster/features/) — Describe supported and finalized feature versions
- [`cluster info`](./cluster/info/) — Describe the cluster: its brokers, controller, and cluster id
- [`cluster log-dirs`](./cluster/log-dirs/) — Describe log directories and their partition sizes, per broker
- [`cluster quorum`](./cluster/quorum/) — Describe the metadata quorum: leader, voters, and observers
- [`cluster raft-voter add`](./cluster/raft-voter-add/) — Add a voter to the KRaft metadata quorum
- [`cluster raft-voter remove`](./cluster/raft-voter-remove/) — Remove a voter from the KRaft metadata quorum
- [`cluster reassign execute`](./cluster/reassign-execute/) — Execute a partition reassignment from a kafka-reassign-partitions.sh-shaped JSON file
- [`cluster reassign list`](./cluster/reassign-list/) — List every active partition reassignment
- [`cluster unregister-broker`](./cluster/unregister-broker/) — Unregister a broker from the cluster (KRaft broker decommissioning)
- [`cluster update-features`](./cluster/update-features/) — Upgrade, safe-downgrade, or unsafe-downgrade one or more finalized feature versions

## config

- [`config describe`](./config/describe/) — Describe the configs of one or more resources
- [`config list-resources`](./config/list-resources/) — List every config resource the broker knows about
- [`config set`](./config/set/) — Set one or more config entries on a resource
- [`config unset`](./config/unset/) — Remove one or more config entries from a resource, reverting them to default

## group

- [`group delete`](./group/delete/) — Delete one or more consumer groups
- [`group delete-offsets`](./group/delete-offsets/) — Delete a consumer group's committed offsets on one or more topics
- [`group describe`](./group/describe/) — Describe one or more consumer groups
- [`group list`](./group/list/) — List every consumer group the cluster knows about
- [`group offsets`](./group/offsets/) — Show a consumer group's committed offsets
- [`group remove-members`](./group/remove-members/) — Remove one or more static members from a consumer group's session
- [`group reset-offsets`](./group/reset-offsets/) — Reset a consumer group's committed offsets on one or more topics

## quota

- [`quota alter`](./quota/alter/) — Set or remove client quota values for one entity
- [`quota describe`](./quota/describe/) — Describe client quotas matching an entity filter

## scram

- [`scram delete`](./scram/delete/) — Delete a SCRAM credential for one or more users
- [`scram list`](./scram/list/) — List SCRAM credentials for one or more users, or every user
- [`scram set`](./scram/set/) — Create or update a SCRAM credential for one or more users

## share-group

- [`share-group delete`](./share-group/delete/) — Delete one or more share groups
- [`share-group describe`](./share-group/describe/) — Describe one or more share groups
- [`share-group list`](./share-group/list/) — List every share group the cluster knows about
- [`share-group offsets`](./share-group/offsets/) — Read, set, or delete a share group’s committed start offsets

## token

- [`token create`](./token/create/) — Create a delegation token
- [`token expire`](./token/expire/) — Expire a delegation token, immediately by default
- [`token list`](./token/list/) — List delegation tokens, optionally filtered by owner
- [`token renew`](./token/renew/) — Renew a delegation token, extending its expiry

## topic

- [`topic add-partitions`](./topic/add-partitions/) — Raise a topic to a new total partition count
- [`topic create`](./topic/create/) — Create one or more topics
- [`topic delete`](./topic/delete/) — Delete one or more topics
- [`topic delete-records`](./topic/delete-records/) — Delete records before a given offset, per partition
- [`topic describe`](./topic/describe/) — Describe one or more topics
- [`topic list`](./topic/list/) — List every topic
- [`topic offsets`](./topic/offsets/) — Show partition offsets for a topic
- [`topic producers`](./topic/producers/) — Show a topic's active producer state, per partition

## txn

- [`txn abort`](./txn/abort/) — Write an abort marker for one in-flight transaction on a topic partition
- [`txn describe`](./txn/describe/) — Describe one or more transactional ids
- [`txn fence`](./txn/fence/) — Fence out a transactional id's current producer, bumping its epoch
- [`txn list`](./txn/list/) — List transactions known to the cluster, optionally filtered
- [`txn terminate`](./txn/terminate/) — Force-terminate a transactional id's current transaction
