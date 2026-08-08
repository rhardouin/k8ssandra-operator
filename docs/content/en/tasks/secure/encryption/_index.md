---
title: "Enabling encryption"
linkTitle: "Enabling encryption"
toc_hide: false
no_list: false
weight: 6
description: Setting up encryption in K8ssandra clusters.
---

Apache Cassandra&reg; offers the ability to encrypt internode communications and client-to-node communications separately. This topic explains how to set up and configure encryption in K8ssandra clusters.

## Prerequisites

* A supported Kubernetes 1.19+ environment, either local (kind, K3D, minikube) or via a cloud provider:
  * Amazon Elastic Kubernetes Service (EKS)
  * DigitalOcean Kubernetes (DOKS)
  * Google Kubernetes Engine (GKE) in a Google Cloud project
  * Microsoft Azure Kubernetes Service (AKS)
* **K8ssandra Operator** has been installed - see the [install]({{< relref "install" >}}) topics
* An SSL encryption store, as covered in the next section

## Generating SSL encryption stores

If you do not have a set of encryption stores available, follow the instructions in [this TLP blog post](https://thelastpickle.com/blog/2021/06/15/cassandra-certificate-management-part_1-how-to-rotate-keys.html). More specifically, use [this script](https://github.com/thelastpickle/cassandra-toolbox/tree/main/generate_cluster_ssl_stores) to generate the SSL stores.

You could clone the [cassandra-toolbox](https://github.com/thelastpickle/cassandra-toolbox) GitHub repository, and create a `cert.conf` file with the following format:

```conf
[ req ]
distinguished_name     = req_distinguished_name
prompt                 = no
output_password        = MyPassWord123!
default_bits           = 3072

[ req_distinguished_name ]
C                      = FR
ST                     = IDF
L                      = Paris
O                      = YourCompany
OU                     = SSLTestCluster
CN                     = SSLTestClusterRootCA
emailAddress           = youraddress@whatever.com
```

Next, run:

```bash
./generate_cluster_ssl_stores.sh -v 10000 -g cert.conf
```

The `-v` value above sets the validity of the generated certificates in days. 

{{% alert title="Tip" color="success" %}}
Don't set this `-v` days value too low. Doing so would require you to rotate the certificates too often; it's not a trivial operation.
{{% /alert %}}

The command output should be a folder containing a keystore, a truststore, and a file containing their respective passwords.

Rename the keystore file to `keystore`, and rename the truststore file to `truststore`. Then create a Kubernetes secret with the following command:

```bash
kubectl create secret generic server-encryption-stores --from-file=keystore --from-literal=keystore-password=<keystore password> --from-file=truststore --from-literal=truststore-password=<truststore password> -o yaml > server-encryption-stores.yaml
```

Replace the `<keystore password>` and `<truststore password>` above with each store's actual password.

{{% alert title="Tip" color="success" %}}
You can repeat the above procedure to generate encryption stores for client-to-node encryption, changing the secret name appropriately.
Check the following section for more details on setting up client encryption with Medusa.
{{% /alert %}}

### Medusa client certificates

In order to work with a cluster with client to node encryption, Medusa will require an additional client certificate to be created.
First, create a `client.conf` file with the matching the root CA conf:

```conf
[ req ]
distinguished_name     = req_distinguished_name
prompt                 = no
output_password        = MyPassWord123!
default_bits           = 3072

[ req_distinguished_name ]
C                      = FR
O                      = YourCompany
OU                     = SSLTestCluster
CN                     = client
```

Then, run the following command to create the certificate/key pair:

```bash
openssl req -newkey rsa:3078 -nodes -keyout client.key -out client.csr -config client.conf
```

When generating the encryption stores in the previous step, you should have created a root CA certificate and key. You can find them in the `certs` folder, with names like `ca_<date_time>.cert` and `ca_<date_time>.key`.
Rename them as : `rootca.crt` and `rootca.key`.   
Now, sign the client certificate using these files:
  
```bash
openssl x509 -req -CA rootca.crt -CAkey rootca.key -passin pass:MyPassWord123! -in client.csr -out client.crt_signed -days 3650 -CAcreateserial
```

Finally, create a Kubernetes secret with the following command:

```bash
kubectl create secret generic client-certificates --from-file=rootca.crt --from-file=client.key --from-file=client.crt_signed  -o yaml > client-certificates.yaml
```


## Creating a cluster with internode encryption

In order to create a K8ssandra cluster with encryption, first create a namespace and the encryption stores secrets previously generated in it.

In the `K8ssandraCluster` manifest, you will need to configure encryption settings in the `config/cassandraYaml` section.

Also, you'll need to reference the encryption stores' secrets under:

* `cassandra/serverEncryptionStores` 
* *Or*`cassandra/clientEncryptionStores`

Server encryption and client encryption are different entities. They both have their own keystore/truststore pair.
The "or" here shows that you can turn on either independently, or both. Server is for internode communications encryption, and client is for client-to-node communications encryption.

Example:

```yaml
apiVersion: k8ssandra.io/v1alpha1
kind: K8ssandraCluster
metadata:
  name: test
spec:
  cassandra:
    serverVersion: "4.0.1"
    storageConfig:
      cassandraDataVolumeClaimSpec:
        storageClassName: standard
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 5Gi
    config:
      cassandraYaml:
        server_encryption_options:
            internode_encryption: all
            require_client_auth: true
            ...
            ...
        client_encryption_options:
            enabled: true
            require_client_auth: true
            ...
            ...
    datacenters:
      - metadata:
          name: dc1
        size: 3
    serverEncryptionStores:
      keystoreSecretRef:
        name: server-encryption-stores
      truststoreSecretRef:
        name: server-encryption-stores
    clientEncryptionStores:
      keystoreSecretRef:
        name: client-encryption-stores
      truststoreSecretRef:
        name: client-encryption-stores
```

Enabling client-to-node encryption will also encrypt JMX communications. Running Cassandra `nodetool` commands will then require additional arguments to pass the encryption stores and their passwords.

{{% alert title="Note" color="success" %}}
Again, server (internode) and client (client-to-node) encryption are totally independent and can be enabled/disabled individually, as well as use different encryption stores.
{{% /alert %}}

## Stargate and Reaper encryption

Stargate and Reaper will both inherit from Cassandra's encryption settings without any additional change to the manifest.

An encrypted cluster with both Stargate and Reaper would be deployed with the following manifest:

```yaml
apiVersion: k8ssandra.io/v1alpha1
kind: K8ssandraCluster
metadata:
  name: test
spec:
  cassandra:
    serverVersion: "4.0.1"
    storageConfig:
      cassandraDataVolumeClaimSpec:
        storageClassName: standard
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 5Gi
    config:
      cassandraYaml:
        server_encryption_options:
            internode_encryption: all
            require_client_auth: true
            ...
            ...
        client_encryption_options:
            enabled: true
            require_client_auth: true
            ...
            ...
    datacenters:
      - metadata:
          name: dc1
        size: 3
    serverEncryptionStores:
      keystoreSecretRef:
        name: server-encryption-stores
      truststoreSecretRef:
        name: server-encryption-stores
    clientEncryptionStores:
      keystoreSecretRef:
        name: client-encryption-stores
      truststoreSecretRef:
        name: client-encryption-stores
  stargate:
    size: 1
  reaper:
    deploymentMode: SINGLE
```

## Medusa encryption

In order to work with a cluster that has client-to-node encryption enabled, Medusa needs to use a matching certificate (consisting of client certificate and key, as well as the root CA certificate) that the Python Driver can use.

This certificate can be generated with the command mentioned above. Result will be a single `Secret` holding all the various components under hard-coded keys.

After having created the `client-certificates` secret, you will need to reference it in the `medusa` section of the `K8ssandraCluster` manifest:

```yaml
...
spec:
  ...
  medusa:
    certificatesSecretRef:
      name: client-certificates
```

However, this aproach is too prescriptive and we decided to deprecate it. As a replacement, we added `clientEncryptionStores` field to `medusa`'s spec. It allows more flexible control of where do Medusa's certificates come from.

To achieve the same behaviour as `certificatesSecretRef` (to use a certificate generated with the command earlier), the spec should look like this:

```yaml
...
spec:
  ...
  medusa:
    clientEncryptionStores:
      keystoreSecretRef:
        name: client-certificates
        key: rootca.crt
      truststoreSecretRef:
        name: client-certificates
        key: client.crt_signed
      truststorePasswordSecretRef:
        name: client-certificates
        key: client.key
```

The `clientEncryptionStores` also feature `KeystorePasswordRef`, which is ignored in this case. When both `certificatesSecretRef` and `clientEncryptionStores` are set, the operator will use `certificatesSecretRef` and log a line about doing so.

## Secure legacy-source discovery

Migration discovery uses dedicated source CQL fields. They are separate from `serverEncryptionStores` and `clientEncryptionStores`, which configure Java keystore/truststore material for managed Cassandra workloads.

### Source credentials

If the legacy source requires CQL authentication, create an `Opaque` Secret in the same namespace as the `K8ssandraCluster`. It must contain non-empty `username` and `password` keys.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: legacy-cql-reader
  namespace: k8ssandra-operator
type: Opaque
stringData:
  username: <legacy-read-only-username>
  password: <legacy-password-from-your-secret-manager>
```

Reference it only through the dedicated field:

```yaml
spec:
  secretsProvider: internal
  cassandra:
    legacyCqlCredentialsSecretRef:
      name: legacy-cql-reader
```

Do not commit literal production credentials. Do not reuse `superuserSecretRef` or another target superuser credential: source authentication and target authentication are independent.

Configure the source authorizer so this identity can issue only the discovery `SELECT` queries against `system.local`, `system.peers_v2`, and `system_schema.keyspaces`. The exact grant syntax depends on the source authorizer; discovery does not need schema-write permission.

The driver authenticates before it can read and verify the source cluster identity. Valid credentials therefore do not prove that an IP belongs to the intended cluster; keep `spec.cassandra.clusterName` accurate and restrict network access to the expected source endpoints.

If these credentials cross any network that is not explicitly trusted, authenticated TLS is required. Password authentication without TLS exposes the source credential to interception. Omitting `legacyCqlTLSSecretRef` is supported only when every hop from the discovery Pod to every `additionalSeeds` endpoint is on a trusted network covered by your organization's security policy.

### Source TLS

When source CQL uses TLS, create a second same-namespace Secret with PEM data:

- `ca.crt` is required.
- `tls.crt` and `tls.key` are optional for one-way TLS, but must be provided together for mutual TLS.

```bash
kubectl create secret generic legacy-cql-tls \
  --from-file=ca.crt=./<source-ca.pem> \
  --from-file=tls.crt=./<optional-client-cert.pem> \
  --from-file=tls.key=./<optional-client-key.pem> \
  -n k8ssandra-operator
```

For one-way TLS, omit both client flags. Reference the Secret with:

```yaml
spec:
  cassandra:
    legacyCqlTLSSecretRef:
      name: legacy-cql-tls
```

Discovery verifies the certificate against the exact contacted IP and rejects configurations that disable certificate verification. Ensure the certificate contains that IP in its subject alternative names. A DNS-only certificate does not validate an IP-literal `additionalSeeds` entry.

### Kubernetes least privilege

The per-attempt worker ServiceAccount can access only its pre-created result ConfigMap in the discovery namespace, with `get` and `patch`. It cannot create ConfigMaps, read Secrets through the Kubernetes API, or list namespace resources. The attempt input, HMAC key, copied credential/TLS material, and result target are mounted or named explicitly.

For the discovery lifecycle, the controller's data-plane identity needs these namespaced permissions:

- core `configmaps`: `create`, `delete`, `get`, `list`, `patch`, `watch`
- core `secrets`: `create`, `delete`, `get`, `list`, `watch`
- core `serviceaccounts`: `create`, `delete`, `get`, `list`, `watch`
- core `pods`: `delete`, `get`, `list`, `watch`
- batch `jobs`: `create`, `delete`, `get`, `list`, `watch`
- rbac.authorization.k8s.io `roles` and `rolebindings`: `create`, `delete`, `get`, `list`, `watch`

On the control plane, the controller needs `get`, `list`, and `watch` on the referenced core `secrets`, plus its existing `K8ssandraCluster` and status permissions. These are the discovery-specific permissions; the complete operator Role also contains permissions for the operator's other controllers. Do not grant the worker the controller Role.

### Secret binding and rotation

At attempt creation, the controller binds each reference by purpose, source Kubernetes context, namespace, name, relevant keys, and Secret `resourceVersion`. It re-reads that fully qualified source before use. Only the required keys are copied into a controller-owned Secret in the selected discovery data-plane namespace and mounted read-only into the bounded Job. Secret bytes are never stored in `K8ssandraCluster` status, Events, or the accepted snapshot.

Changing either referenced Secret invalidates stale work and requeues the cluster. The controller authoritatively re-reads both Secrets and their `resourceVersion` values immediately before the acceptance compare-and-swap; a rotation at that boundary prevents acceptance. Recoverable credential and TLS failures retry automatically with bounded backoff. After snapshot acceptance, the attempt Job, Pods, copied Secret, HMAC Secret, result and input ConfigMaps, ServiceAccount, Role, and RoleBinding are deleted asynchronously; managed-datacenter authorization remains in the accepted status, which retains binding metadata but no Secret data.

`secretsProvider: external` is not supported for a discovery-aware migration. Synchronize the required source Secrets into the `K8ssandraCluster` namespace using your approved secret-delivery process, then reference the Kubernetes Secret by name.

## Next steps

Explore other K8ssandra [tasks]({{< relref "/tasks" >}}).

See the [Reference]({{< relref "/reference" >}}) topics for information about K8ssandra charts, and a glossary. 
