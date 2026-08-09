---
title: "Troubleshoot K8ssandra Operator"
linkTitle: "Troubleshoot"
description: "Troubleshooting tips for K8ssandra Operator users."
---

The best place to start when troubleshooting a K8ssandra cluster deployment is its status. The status of a K8ssandra
cluster reports useful information about each of its components (CassandraDatacenter, Stargate, Reaper, etc.)


## Checking reconcile errors

If the last reconcile failed, the error will show up in the `.status.error` field of the `K8ssandraCluster` resource, which is displayed by default when listing K8ssandraCluster resources:

```bash
kubectl get k8c 
```

**Output:**

```bash
NAME      ERROR
demo      None
```

If the error is `None`, then the last reconcile was successful. Otherwise, the error message will be displayed.

Reconcile errors are also notified in the Kubernetes events:

```bash
% kubectl describe k8c demo
Name:        demo
...
...
Events:
  Type     Reason  Age   From                     Message
  ----     ------  ----  ----                     -------
  Warning  Error   2m    k8ssandra-operator 1.0  reconcile failed: failed to create CassandraDatacenter: admission webhook "cassandra.datastax.com" denied the request: CassandraDatacenter.cassandra.datastax.com "demo-dc1" is invalid: spec.serverType: Unsupported value: "bogus": supported values: "dse", "cassandra"
```

## Troubleshoot legacy replication discovery

New `K8ssandraCluster` migrations with non-empty `spec.cassandra.additionalSeeds` are gated before managed Cassandra creation. Check their discovery state with:

```bash
kubectl get k8c <cluster_name> -n <namespace> \
  -o jsonpath='{.status.legacyRFDiscovery.phase}{"\t"}{.status.legacyRFDiscovery.reason}{"\t"}{.status.legacyRFDiscovery.message}{"\n"}'
kubectl get events -n <namespace> \
  --field-selector involvedObject.kind=K8ssandraCluster,involvedObject.name=<cluster_name> \
  --sort-by=.lastTimestamp
```

`Pending` means that a bounded discovery attempt is in progress. `Blocked` reports one of the finite reasons below. `Accepted` means the immutable snapshot passed readback; it does not mean the managed datacenter is Ready. Identical phase/reason/message states do not update `lastTransitionTime` or emit duplicate Events, so use current status as the primary signal rather than expecting periodic Events.

`ExternalReplicationDrift` is different: discovery remains `Accepted`, while the separate `SystemKeyspaceReplicationReady` condition becomes `False`.

```bash
kubectl get k8c <cluster_name> -n <namespace> \
  -o jsonpath='{range .status.conditions[?(@.type=="SystemKeyspaceReplicationReady")]}{.status}{"\t"}{.reason}{"\t"}{.message}{"\n"}{end}'
```

If CREATE fails during an operator upgrade, wait for the rollout to finish so that a single manager Pod is serving admission before submitting discovery-marked objects again. Leader election keeps one reconciler active even while two Pods overlap during a rolling update. Do not bypass a failing webhook or manually run old and new reconcilers together; mixed-version operation is unsupported.

### Stable reasons and recovery

The Retry column matches the controller's public failure contract. “Automatic” means the controller retries with bounded backoff after the prerequisite recovers; some permanent failures require a corrected new migration object.

| Reason | Surface | Retry | Safe corrective action |
|---|---|---:|---|
| `AdmissionUnavailable` | CREATE admission response; no object status | Automatic client retry | Restore the mutating webhook service, endpoints, certificate, and API-server reachability, then submit CREATE again. Do not bypass the webhook. |
| `MarkerInvalid` | `Blocked` | No | Do not add or edit `k8ssandra.io/legacy-rf-discovery-version`. Recreate through supported admission. |
| `UnsupportedServerType` | `Blocked` or CREATE rejection | No | Use this workflow only with `serverType: cassandra`; DSE and HCD are unsupported. |
| `UnsupportedSourceVersion` | `Blocked` | No | Upgrade the legacy source to Apache Cassandra 4.0 or newer before migrating. |
| `UnsupportedSecretsProvider` | `Blocked` or CREATE rejection | No | Set `spec.secretsProvider: internal`; external providers are unsupported for discovery. |
| `DiscoveryTooLate` | `Blocked` | No | Managed Cassandra state already exists. Stop and use a reviewed rollback/recreation procedure; discovery cannot be started retroactively. |
| `InvalidContactPoint` | `Blocked` or CREATE rejection | No | Use IP literals only, without ports, zones, or surrounding whitespace. |
| `JobSchedulingFailed` | `Blocked` | Automatic | Inspect data-plane scheduling Events, quota, admission policy, node selectors, and service-account availability. |
| `WorkerImageUnavailable` | `Blocked` | Automatic | Restore the configured discovery worker image reference. |
| `WorkerImagePullFailed` | `Blocked` | Automatic | Restore registry reachability and image-pull authorization in the discovery data plane. |
| `DiscoveryDeadlineExceeded` | `Blocked` | Automatic | Restore network/source responsiveness; the Job has a bounded deadline and no Job-level retry. |
| `CredentialSecretInvalid` | `Blocked` | Automatic | In the `K8ssandraCluster` namespace, restore the referenced Secret with non-empty `username` and `password` keys. |
| `TLSMaterialInvalid` | `Blocked` | Automatic | Restore PEM `ca.crt`; if using mutual TLS, provide both `tls.crt` and `tls.key`. |
| `AuthenticationRejected` | `Blocked` | Automatic | Correct the dedicated source credential and confirm the source authenticator accepts it. |
| `AuthorizationDenied` | `Blocked` | No | Grant the discovery identity the required `SELECT` permissions on `system.local`, `system.peers_v2`, and `system_schema.keyspaces`. |
| `TLSFailed` | `Blocked` | Automatic | Correct the CA/client material and make the certificate valid for the contacted IP. Verification cannot be disabled. |
| `ContactUnreachable` | `Blocked` | Automatic | Restore data-plane connectivity to at least one ordered source IP on TCP 9042. |
| `IdentityMismatch` | `Blocked` | No | Point seeds at the intended cluster and make `spec.cassandra.clusterName` (or `metadata.name`) match its Cassandra cluster name. |
| `SchemaDisagreement` | `Blocked` | Automatic | Wait for source schema agreement and stop concurrent schema changes, then let discovery retry. |
| `TopologyInconsistent` | `Blocked` | Automatic | Repair incomplete or conflicting source topology metadata and wait for it to stabilize. |
| `ManagedDatacenterNameCollision` | `Blocked` | No | Rename the planned managed datacenter so its Cassandra datacenter name does not collide with an observed external name. |
| `MissingKeyspace` | `Blocked` | No | Restore all of `system_auth`, `system_traces`, and `system_distributed`; discovery never creates them. |
| `UnsupportedStrategy` | `Blocked` | No | Convert each required system keyspace to `NetworkTopologyStrategy` before retrying. |
| `InvalidReplication` | `Blocked` | No | Correct malformed RF metadata. Present RF values must be ASCII base-10 integers in `1..2147483647`; sparse maps are allowed. |
| `StaleDiscoveryResult` | `Blocked` | Automatic | Allow the stale attempt to be discarded and retried against the current generation, seeds, Secret versions, and plan. |
| `InvalidDiscoveryResult` | `Blocked` | Automatic | Inspect worker/controller health and allow the invalid result to be discarded. Do not retrieve or edit `result.json`. |
| `ForgedDiscoveryResult` | `Blocked` | Automatic | Remove unauthorized writers from the attempt namespace, restore controller-managed resources, and allow a clean attempt. |
| `DiscoveryResultTooLarge` | `Blocked` | Automatic | Check for abnormal source topology/result growth; results above the controller limit are rejected. Do not collect the result body. |
| `KubernetesAPIUnavailable` | `Blocked` | Automatic | Restore control-plane/data-plane API access and referenced `ClientConfig` connectivity. |
| `KubernetesAPIConflict` | `Blocked` | Automatic | Allow reconciliation to retry after concurrent object/status updates settle. |
| `SnapshotConflict` | `Blocked` | No | If any managed DC exists or existed, stop: rediscovery is forbidden for this object. Before managed creation, the controller may rediscover only after authoritative absence checks succeed. |
| `ManagedStatePresent` | `Blocked` | No | Managed Cassandra state already exists in a current or historical location. Do not delete evidence to force rediscovery. |
| `ExternalReplicationDrift` | `Accepted` plus `SystemKeyspaceReplicationReady=False` | Automatic after correction | Compare all three live external maps with `acceptedSnapshot.replication`, correct the externally owned drift deliberately, and let the operator re-read. It issues zero DDL while drift exists. |

Inspect attempt infrastructure without exposing its Secret data or result body:

```bash
kubectl get jobs,pods -n <data-plane-namespace> \
  -l k8ssandra.io/cluster-name=<cluster_name>,k8ssandra.io/cluster-namespace=<namespace>
kubectl describe job -n <data-plane-namespace> <discovery-job>
kubectl logs -n <data-plane-namespace> job/<discovery-job>
kubectl get k8c <cluster_name> -n <namespace> \
  -o jsonpath='{.status.legacyRFDiscovery.acceptedSnapshot.attemptTrace}'
```

Logs, status, Events, and the accepted attempt trace are sanitized. Do not print credential/TLS Secrets, the HMAC Secret, attempt inputs, or result ConfigMap bodies. Attempt Jobs, Pods, copied inputs, and results are normally cleaned up after acceptance, so their absence after `Accepted` is expected.

### Snapshot recovery boundary

Before any managed `CassandraDatacenter` exists, missing or corrupt accepted state can trigger a full rediscovery only after authoritative absence checks cover both current and historical managed locations. Once `managedCreationObserved` is true, or any current/historical absence check is ambiguous or finds state, snapshot loss is a permanent conflict for that object. Deleting a DC or removing it from the current specification does not reset this history.

## Inspecting the cluster status

The cluster status can be obtained with the following command (executed in the appropriate namespace):

    kubectl describe k8c <cluster_name>

### Overall status

A `K8ssandraCluster` status has the following overall structure:

```
Status:
  Conditions: ...         # conditions applying to the whole cluster – see below
  Decommission Progress:  # decommission progress, if a datacenter is being decommissioned – see below 
  Datacenters:            # status of each managed datacenter in this cluster, keyed by name
    <datacenter_name>:
      Cassandra: ...      # status of the datacenter itself (always present)
      Reaper: ...         # status of Reaper, if deployed in this datacenter, absent otherwise
      Stargate: ...       # status of Stargate, if deployed in this datacenter, absent otherwise
```

The `Datacenters` entry is a map keyed by datacenter name. Each datacenter reports its own status per component:
currently Cassandra, Reaper and Stargate statuses are included.

### CassandraDatacenter status

The `Cassandra` entry of a datacenter status section is provided by cass-operator. The contents of this entry correspond
to the status of the `CassandraDatacenter` resource, and provide useful information about the Cassandra cluster and its
nodes.

When the datacenter is ready, the status of this entry looks like below:

```
# Status.Datacenters.<datacenter_name>:
  Cassandra:
    Cassandra Operator Progress:  Ready
    Conditions: ...
    Node Statuses:
      <pod_name>:
      <pod_name>:
      ...
    Observed Generation:  1
    Quiet Period:         2022-02-28T17:14:16Z
    Super User Upserted:  2022-02-28T17:14:11Z
    Users Upserted:       2022-02-28T17:14:11Z
```

Check cass-operator documentation for more information about the `CassandraDatacenter` resource status, and 
especially about all the conditions available, and their meanings.

### Reaper status

The `Reaper` entry of a datacenter status is provided by k8ssandra-operator. The contents of this entry correspond to
the status of the `Reaper` resource.

When Reaper is being deployed, this entry usually looks like below:

```
# Status.Datacenters.<datacenter_name>:
  Reaper:
    Conditions:
      Last Transition Time:  2022-02-28T17:20:04Z
      Status:                False
      Type:                  Ready
    Progress:                Configuring
```

Currently, Reaper only supports the `Ready` condition; it is set to true when Reaper is ready.

The `Progress` field can have the following values: 

* `Pending`: when the controller is waiting for the `CassandraDatacenter` to become ready.
* `Deploying`: when controller is waiting for the Reaper deployment and its associated service to become ready.
* `Configuring`: when the Reaper instance is ready for work and is being connected to its target datacenter.
* `Running`: when Reaper is up and running.

When Reaper is ready, the status of this entry looks like below:

```
# Status.Datacenters.<datacenter_name>:
  Reaper:
    Conditions:
      Last Transition Time:  2022-02-28T17:22:35Z
      Status:                True
      Type:                  Ready
    Progress:                Running    
```

When Reaper is fully deployed, the `Ready` condition must be true, and the `Progress` field must be set to `Running`.

### Stargate status

The `Stargate` entry of a datacenter status is provided by k8ssandra-operator. The contents of this entry correspond to
the status of the `Stargate` resource.

When Stargate is being deployed, this entry usually looks like below:

```
# Status.Datacenters.<datacenter_name>:
  Stargate:
    Available Replicas:  0
    Conditions:
      Last Transition Time:  2022-02-28T17:22:42Z
      Status:                False
      Type:                  Ready
    Deployment Refs:
      <stargate_deployment_ref>
      <stargate_deployment_ref>
      ...
    Progress:              Deploying
    Ready Replicas:        0
    Ready Replicas Ratio:  0/3
    Replicas:              3
    Updated Replicas:      3
```

Currently, Stargate only supports the `Ready` condition; it is set to true when Stargate is ready.

The `Progress` field can have the following values:

* `Pending`: when the controller is waiting for the datacenter to become ready.
* `Deploying`: when the controller is waiting for the Stargate deployment and its associated service to become ready.
* `Running`: when Stargate is up and running.

When Stargate is ready, the status of this entry looks like below:

```
# Status.Datacenters.<datacenter_name>:
  Stargate:
    Available Replicas:  3
    Conditions:
      Last Transition Time:  2022-02-28T17:20:01Z
      Status:                True
      Type:                  Ready
    Deployment Refs:
      <stargate_deployment_ref>
      <stargate_deployment_ref>
      ...
    Progress:              Running
    Ready Replicas:        3
    Ready Replicas Ratio:  3/3
    Replicas:              3
    Service Ref:           <service_ref>
    Updated Replicas:      3
```

When Stargate is fully deployed, the `Ready` condition must be true, and the `Progress` field must be set to `Running`.

### Available `K8ssandraCluster` conditions

Currently, the only condition supported at K8ssandraCluster level is `CassandraInitialized`: it is set to true when the
Cassandra cluster (that is, the Cassandra nodes without taking into account other components, such as Stargate or
Reaper) becomes ready for the first time. During the lifetime of that Cassandra cluster, datacenters may have their
readiness condition change back and forth. Once set, this condition however does not change. This condition is mainly
intended for internal use.

### Decommission Progress

The field `Decommission Progress` is only set when there is an ongoing datacenter decommission. When non-empty, it can
have the following values:

* `UpdatingReplication`: in this phase, keyspace replications are being updated to reflect the datacenter decommission.
* `Decommissioning`: this phase is carried out by cass-operator and corresponds to the actual datacenter decommission.

## Check quotas

In some cases, pods can become "unhealthy" and the root cause may be an insufficient quota. You can check quotas in the cloud provider's UI. For example, in the Google Cloud Platform (GCP) console, check for any unhealthy pods in your GKE project. Then in the IAM &amp; Admin section of the GCP console, navigate to Quotas. Look for any reported issues with backend services:

![Backend service quota error](gcp-quota-example1.png)

From the GCP &gt; IAM &amp; Admin &gt; Quotas display:

1. Select the row for the service name that is reporting a quota issue
2. Click **All Quotas** from the Details column
3. Check the box for the affected quota, and click **Edit Quota**. 
4. The dialog indicates: "Enter a new quota limit. Your request will be sent to your service provider for approval." Examine the displayed current value and set a new value. 
5. Enter a brief request description and click **Next**.
6. Verify your contact information, and click **Submit Request**.

Notice how in the following example the Backend services quota is set to '5', and we're changing it to '50'. For the K8ssandra deployments (Stargate, cass-operator, Reaper, Medusa, and so on), actually `10` might be a sufficient quota.

![Quota UI showing change in Backend service quota from 5 to 50](gcp-quota-example2.png)


## Bucket region or name for backups is misconfigured

Among the operators installed by K8ssandra is Medusa, which provides backup and restore for Cassandra data.

If the storage object's name or region used by an Amazon S3 bucket does not match the values expected by Medusa, an error is written to the Medusa section of the logs.  Example:

```bash
kubectl logs demo-dc1-default-sts-0 -c medusa
.
.
.
File "/usr/local/lib/python3.6/dist-packages/libcloud/storage/drivers/s3.py", line 143, in parse_error driver=S3StorageDriver)
libcloud.common.types.LibcloudError: <LibcloudError in <class 'libcloud.storage.drivers.s3.S3StorageDriver'> 
'This bucket is located in a different region. Please use the correct driver. Bucket region "us-east-2", used region "us-east-1".'>
As a result of the region mismatch, the Medusa container within the <cluster-name>-dc1-default-sts-0 pod fails to start. While other pods launched by the K8ssandra install may start successfully, the <cluster-name>-dc1-default-sts-0 pod will not due to the Medusa error.
```

Separately in Amazon AWS, confirm that you know the correct region and name to use for your bucket. Example:

![Amazon S3 Bucket Overview shows the name of the region](amazon-s3-bucket-overview.png)

Declare the appropriate name and region in a values YAML. For example, create a file named `my-backup-restore-values.yaml`. Notice below the `storage_properties` setting for the region `us-east-1`, which matches the region configured and shown in the Amazon S3 user interface:

```yaml
size: 3
backupRestore: 
  medusa:
    enabled: true
    bucketName: jsmart-k8ssandra-bucket2
    bucketSecret: medusa-bucket-key
    storage: s3
    storage_properties:
      region: us-east-1
```

Also make sure the bucketName matches: `jsmart-k8ssandra-bucket2`, in this example.

For example, relreferring again to the S3 UI, confirm the bucket name:

![Confirm the bucket name as shown in the Amazon S3 UI](amazon-s3-confirm-bucket-name.png)

Then for a new or existing K8ssandra installation, reference the values file. 

New install:

```bash
helm install demo k8ssandra/k8ssandra -f my-backup-restore-values.yaml
```

Upgrade:

```bash
helm upgrade demo k8ssandra/k8ssandra -f my-backup-restore-values.yaml
```

{{% alert title="Tip" color="success" %}}
If you're using Google Cloud Storage for your backups, you do not need to include the region setting in a values YAML. 
{{% /alert %}}


## Incorrect credentials are configured for backups

If the Medusa log reports an authentication error, check that you provided the correct credentials. For example, with Amazon S3 buckets, check the credentials in the configured `aws_access_key_id` and `aws_secret_access_key` settings. 

For example, `my-medusa-bucket-key.yaml` contains:

```yaml
apiVersion: v1
kind: Secret
metadata:
 name: medusa-bucket-key
type: Opaque
stringData:
# Note that this currently has to be set to medusa_s3_credentials!
medusa_s3_credentials: |-
  [default]
  aws_access_key_id = FakeValues99ESPW3ALMEZ6U
  aws_secret_access_key = FakeValues99cl9bqJFVA3iFUm+yqVe08HxhXFE/ilK
``` 

If your IT group manages S3 credentials, contact IT to get the correct values.

Before installing or upgrading K8ssandra, and before starting a backup, apply the Medusa bucket values to your Kubernetes environment. Example:

```bash
kubectl apply -f my-medusa-bucket-key.yaml
```

**Output:**

```bash
 secret/medusa-bucket-key configured
```

## Collect useful information

Suppose you have an error after editing a K8ssandra configuration, or you want to inspect some things as you learn.  There are some useful commands that come in handy when needing to dig a bit deeper. The following examples assume you are using a `k8ssandra` namespace, but this can be adjusted as needed.

Issue the following `kubectl` command to view the `Management-api` logs.  Replace *cassandra-pod* with an actual pod instance name:

```bash
kubectl logs *cassandra-pod* -c cassandra -n k8ssandra
```

Issue the following `kubectl` command to view the `Cassandra` logs.  Replace *cassandra-pod* with an actual pod instance name:

```bash
kubectl logs *cassandra-pod* -c server-system-logger -n k8ssandra
```

Issue the following `kubectl` command to view `Medusa` logs.  Replace *cassandra-pod* with an actual pod instance name:

```bash
kubectl logs *cassandra-pod* -c medusa -n k8ssandra
```

Issue the following `kubectl` command to describe the `CassandraDatacenter` resource.  This provides a wealth of information about the resource, which includes `aged events` that assist when trying to troubleshoot an issue:

```bash
kubectl describe cassandradatacenter/dc1 -n k8ssandra
```

Gather container specific information for a pod.

 First, list out the pods scoped to the K8ssandra namespace and instance with a target release:

```bash
kubectl get pods -l app.kubernetes.io/instance=*release-name* -n k8ssandra
```

{{% alert title="Note" color="success" %}}
If you don't know the release name, look it up with:
```bash
helm list -n k8ssandra
```
{{% /alert %}}

Next, targeting a specific pod, filter out `container` specific information. Replace the name of the pod with the pod of interest:

```bash
kubectl describe pod/*pod-name* -n k8ssandra | grep container -C 3
```

A slight variation: list out pods having the label for a `cassandra` cluster:

```bash
kubectl get pods -l cassandra.datastax.com/cluster=*release-name* -n k8ssandra
```

Now, using a pod-name returned, describe all the details:

```bash
kubectl describe pod/*pod-name* -n k8ssandra
```

## Next steps

* Explore other K8ssandra Operator [tasks]({{< relref "/tasks" >}}).
* See the [Reference]({{< relref "/reference" >}}) topics for information about K8ssandra Operator Custom Resource Definitions (CRDs) and the single K8ssandra Operator Helm chart.  
