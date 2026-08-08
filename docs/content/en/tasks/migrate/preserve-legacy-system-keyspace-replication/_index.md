---
title: "Preserve legacy system-keyspace replication"
linkTitle: "Preserve legacy replication"
no_list: true
weight: 2
description: Preserve existing system-keyspace replication while adding the first K8ssandra-managed datacenter.
---

## Why preservation is required

An existing datacenter can use a replication factor that differs from the new managed topology. For example, changing `system_auth` from RF 2 to RF 3 before the new datacenter has replicas can make authentication unavailable during migration.

Legacy replication discovery preserves the complete `NetworkTopologyStrategy` maps observed for exactly these keyspaces:

- `system_auth`
- `system_traces`
- `system_distributed`

The maps remain independent. The operator does not normalize their existing datacenter entries to RF 3.

## Complete the operator rollout first

{{% alert title="Required rollout order" color="warning" %}}
Do not create or modify a legacy-RF migration until the K8ssandra Operator Deployment rollout has fully completed and no older manager is still running. Mixed old and new reconcilers are unsupported for this workflow.
{{% /alert %}}

This order is required before creating the `K8ssandraCluster`, not after discovery has started.

## Configure discovery

Use this workflow only when creating a new `K8ssandraCluster`, before any managed `CassandraDatacenter` has been created. Adding seeds to an existing migration does not activate discovery retroactively.

Before creating the migration resource:

1. Confirm that all three keyspaces exist and use `NetworkTopologyStrategy`. Do not alter their current replication maps.
2. Configure `spec.cassandra.additionalSeeds` with reachable source Cassandra IP addresses. These addresses provide connectivity; they do not define replication topology.
3. If source CQL authentication is enabled, create a namespace-local Secret with non-empty `username` and `password` keys and reference it with `legacyCqlCredentialsSecretRef`.
4. If source CQL TLS is enabled, create a namespace-local Secret containing `ca.crt`. When client certificates are required, also provide both `tls.crt` and `tls.key`, then reference it with `legacyCqlTLSSecretRef`.

```yaml
spec:
  cassandra:
    additionalSeeds:
      - "192.0.2.10"
      - "192.0.2.11"
    legacyCqlCredentialsSecretRef:
      name: legacy-cql-reader
    legacyCqlTLSSecretRef:
      name: legacy-cql-tls
  externalDatacenters:
    - dc1
```

`externalDatacenters` describes replication topology and, when set, must match the external datacenters observed by discovery. It is not a replacement for `additionalSeeds` connectivity.

Discovery is a pre-creation gate. The operator reads the three live replication maps, accepts one snapshot, revalidates it, and only then creates the first managed `CassandraDatacenter`. If discovery is blocked or the external replication entries drift, the operator does not issue system-keyspace DDL.

## Observe the gate

Check the public discovery phase and reason:

```bash
kubectl get k8c <cluster-name> -n <namespace> \
  -o jsonpath='{.status.legacyRFDiscovery.phase}{"\t"}{.status.legacyRFDiscovery.reason}{"\n"}'
```

Do not proceed until the phase is `Accepted`. After the managed Cassandra nodes are healthy, verify the three live replication maps, complete the required rebuild or repair work, and check ring and authentication health before moving client traffic. Discovery preserves replication configuration; it does not prove that replicas have streamed successfully.
