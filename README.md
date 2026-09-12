# Fault-Tolerant Sharded Key-Value Store

A distributed key-value store built in Go that remains consistent and available across concurrent client requests, server failures, retries, and changes in cluster membership. The system combines Paxos-based replication with dynamic sharding to distribute data across replica groups while preserving correct request semantics during reconfiguration.

## Key Features

- **Fault-tolerant replication:** Replicates operations within each server group using Paxos consensus so replicas agree on a single, consistent operation order.
- **Dynamic sharding:** Partitions the key space across replica groups and automatically rebalances shards as groups join or leave the system.
- **Safe shard migration:** Transfers shard data and request history between groups during configuration changes without losing or duplicating operations.
- **Exactly-once request handling:** Uses unique client and request identifiers to detect retries and prevent duplicate `Put` and `Append` operations.
- **Concurrent operation:** Supports multiple clients issuing requests while servers fail, restart, or move between configurations.

## Architecture

The system is organized into four primary components:

| Component | Responsibility |
| --- | --- |
| `paxos` | Implements distributed consensus among replicas. |
| `paxosrsm` | Applies Paxos-decided operations to a replicated state machine in a consistent order. |
| `shardmaster` | Maintains configurations and balances shards as replica groups join, leave, or move. |
| `shardkv` | Stores key-value data, processes client requests, and migrates shards between groups. |

Shared RPC definitions and utilities are located in `common`.

## How It Works

1. A client sends a `Get`, `Put`, or `Append` request to the replica group responsible for the key's shard.
2. The group uses Paxos to agree on the request's position in its replicated operation log.
3. Every replica applies decided operations in the same order and records completed request IDs for deduplication.
4. The shardmaster issues a new configuration when replica groups join or leave.
5. Affected groups transfer shard data and request metadata before serving the new configuration.

## Correctness Challenges

The most interesting part of this project was maintaining consistency across failures and configuration changes. A request may be retried while its shard is moving, a server may miss earlier consensus decisions, or two groups may temporarily hold data associated with different configurations. The implementation coordinates operation ordering, state transfer, and duplicate detection so that each completed client operation has one logical effect.

## Technologies

- Go
- Paxos consensus
- Remote procedure calls (RPC)
- Replicated state machines
- Concurrent and distributed systems

## Repository Structure

```text
.
├── common/       # Shared types and utilities
├── paxos/        # Paxos consensus implementation
├── paxosrsm/     # Replicated state machine layer
├── shardmaster/  # Configuration management and shard balancing
├── shardkv/      # Sharded key-value service and data migration
└── go.mod        # Go module definition
```

## Running the Project

This repository contains the implementation and tests for each distributed-system component. Run all available Go tests from the repository root with:

```bash
go test ./...
```

Individual packages can also be tested separately:

```bash
go test ./paxos
go test ./paxosrsm
go test ./shardmaster
go test ./shardkv
```

## What I Learned

Building this system required reasoning about concurrency, partial failures, consensus, idempotency, and distributed state transitions. It reinforced that failures in distributed systems rarely stay isolated: correctness depends on how replication, request retries, and reconfiguration interact across components.
