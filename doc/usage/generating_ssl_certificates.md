# Generating certificates

If you wish to enable TLS authentication for either etcd or etcdbr server or both, please follow this guide. The SSL certificate configurations given here are meant to facilitate smooth deployment of the etcd setup via the provided [helm chart](../../chart/etcd-backup-restore).

## Certificates structure

While deploying the etcd setup via the provided helm chart, TLS can be enabled for the etcd server and/or etcd-backup-restore server by adding the certificate data to the `values.yaml` file as necessary. This data is converted into the respective secrets and mounted onto the pod's containers according to the following directory structure:

- `etcd` container

```console
/
└── var
    ├── etcd                      Contains the CA and server TLS certs for etcd server
    |   └── ssl
    |       ├── ca
    |       |   └── ca.crt
    |       └── tls
    |           ├── tls.crt
    |           └── tls.key
    └── etcdbr                    Contains the CA and server TLS certs for etcd backup-restore server
        └── ssl
            ├── ca
            |   └── ca.crt
            └── tls
                ├── tls.crt
                └── tls.key
```

<br>

- `backup-restore` container

```console
/
└── var
    ├── etcd                      Contains the CA and server certs for etcd server
    |   └── ssl
    |       ├── ca
    |       |   └── ca.crt
    |       └── tls
    |           ├── tls.crt
    |           └── tls.key
    └── etcdbr                    Contains the CA cert for etcd backup-restore server
        └── ssl
            └── ca
                └── ca.crt
```

## Generating the certificates

### Installing openssl

```console
# For Mac users
brew install openssl

# For other flavours of Unix
apk install openssl

mkdir openssl && cd openssl
```

### Generating certs for etcd server authentication

#### Generating CA cert bundle

```console
openssl genrsa -out ca.key 2048
openssl req -new -key ca.key -subj "/CN=etcd" -out ca.csr

cat > ca.csr.conf <<EOF
[ v3_ext ]
keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign,cRLSign
basicConstraints=critical,CA:TRUE
EOF

openssl x509 -req -in ca.csr -signkey ca.key -out ca.crt -sha256 -days 3653 -extensions v3_ext -extfile ca.csr.conf

# view contents of the generated certificate
openssl x509 -in ca.crt -noout -text
```

#### Generating TLS key-pair

```console
openssl genrsa -out server.key 2048

# In the `alt_names` section of server.csr.conf, replace all occurrences of `mynamespace` with the namespace into which you'll deploy the helm chart
cat > server.csr.conf <<EOF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
CN = etcd-server

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = main-etcd-0
DNS.2 = main-etcd-client
DNS.3 = main-etcd-client.mynamespace
DNS.4 = main-etcd-client.mynamespace.svc
DNS.5 = main-etcd-client.mynamespace.svc.cluster.local

[ v3_ext ]
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
basicConstraints=critical,CA:FALSE
subjectAltName=@alt_names
EOF

openssl req -new -key server.key -out server.csr -config server.csr.conf

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -sha256 -days 3653 -extensions v3_ext -extfile server.csr.conf

# view contents of the generated certificate
openssl x509 -in server.crt -noout -text
```

### Generating certs for etcdbr server authentication

Follow the same steps as [generating certs for etcd](#generating-certs-for-etcd-server-authentication), but replace all occurrences of `etcd` with `etcdbr` for the `CN` fields and replace `main-etcd-client` with `main-backup-client` in the DNS names if you want to access the TLS-enabled backup-restore server via service. You will also need to add `localhost` to the SAN DNS list if you're deploying the etcd setup via the provided helm chart. If deploying by any other means, or if testing locally, please tweak the config accordingly.

#### Generating CA cert bundle

Follow the same steps as [generating CA cert for etcd](#generating-CA-cert-bundle), but replace`CN=etcd` by `CN=etcdbr` while creating the `ca.csr`.

#### Generating TLS key-pair

Follow the same steps as [generating TLS key-pair for etcd](#generating-tls-key-pair), but modify the `[ dn ]` section in `server.csr.conf` from `CN = etcd` by `CN = etcdbr`. Also change the `[ alt_names ]` section to the following:

```console
[ alt_names ]
DNS.1 = localhost
DNS.2 = main-backup-0
DNS.3 = main-backup-client
DNS.4 = main-backup-client.mynamespace
DNS.5 = main-backup-client.mynamespace.svc
DNS.6 = main-backup-client.mynamespace.svc.cluster.local
```

Here, we add `localhost` to the DNS entries so that the etcd bootstrap script may be allowed to trigger data initialization on the backup sidecar via HTTPS.

## Running `etcdbrctl server` with TLS enabled

If you wish to develop/test `etcdbrctl` locally with TLS enabled, you can follow the steps to create the certs and pass them to the `etcdbrctl server` via the following flags.

### For etcd TLS

Pass the CA certificate file via `--cacert` flag, and etcd server TLS certificate and key via `--cert` and `--key` flags respectively.

### For etcdbr TLS

Pass the etcd backup-restore server TLS certificate and key via `--server-cert` and `--server-key` respectively.
