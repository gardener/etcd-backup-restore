<%
  import os

  if context.get("cloud", "") == "":
    raise Exception("missing --var cloud={aws azure gcp openstack local} flag")

  if context.get("container", "") == "":
   raise Exception("missing --var container=ContainerName flag")  
  provider=""
  imageTag="0.2.3"
  if cloud == "aws":
    provider="S3"
  elif cloud == "azure" or cloud == "az":
    provider="ABS"
  elif cloud == "gcp":
    provider="GCS"
  elif cloud == "openstack" or cloud == "os":
    provider="Swift"
  elif cloud == "local":
    provider=""
    region="local"
%>---
apiVersion: v1
kind: Service
metadata:
  name: etcd-client-${cloud}
  labels:
    app: etcd
spec:
  ports:
  - port: 2379
    name: client
    protocol: TCP
  clusterIP: None
  selector:
    app: etcd
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: etcd-bootstrap-${cloud}
  labels:
    app: etcd
data:
  bootstrap.sh: |-
    #!/bin/sh
    while true;
    do
      wget http://localhost:8080/initialization/status -S -O status;
      STATUS=`cat status`;
      case $STATUS in
      "New")
            wget http://localhost:8080/initialization/start -S -O - ;;
      "Progress")
            sleep 1;
            continue;;
      "Failed")
            continue;;
      "Successful")
            exec etcd --data-dir=/var/etcd/data --name=etcd --advertise-client-urls=http://0.0.0.0:2379 --listen-client-urls=http://0.0.0.0:2379 --initial-cluster-state=new --initial-cluster-token=new
            ;;
      esac;
    done
---
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  name: etcd-${cloud}
spec:
  selector:
    matchLabels:
      app: etcd-${cloud}
  serviceName: "etcd-${cloud}"
  replicas: 1
  template:
    metadata:
      labels:
        app: etcd-${cloud} 
    spec:
      containers:
      - name: etcd
        command:
        - /bootstrap/bootstrap.sh
        image: quay.io/coreos/etcd:v3.3.1
        imagePullPolicy: IfNotPresent
        livenessProbe:
          exec:
            command:
            - /bin/sh
            - -ec
            - ETCDCTL_API=3 etcdctl get foo
          initialDelaySeconds: 5
          periodSeconds: 5
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        ports:
        - containerPort: 2380
          name: server
          protocol: TCP
        - containerPort: 2379
          name: client
          protocol: TCP
        volumeMounts:
        - mountPath: /var/etcd/data
          name: etcd-data-${cloud}
        - mountPath: /bootstrap
          name: etcd-bootstrap
      - name: backup
        command:
        - etcdbrctl
        - server
        - --schedule=*/5 * * * *
        - --max-backups=5
        - --data-dir=/var/etcd/data
        - --insecure-transport=true
        - --storage-provider=${provider}
        - --delta-snapshot-period-seconds=10
        - --garbage-collection-period-seconds=60
        image: eu.gcr.io/gardener-project/gardener/etcdbrctl:${imageTag}
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
          name: server
          protocol: TCP
        env:
        - name: STORAGE_CONTAINER
          value: ${container}  # Change the container name here
        % if cloud == "aws" :
        - name: AWS_REGION
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: region
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: secretAccessKey
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: accessKeyID
        % elif cloud == "azure" or cloud == "az" :
        - name: STORAGE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: storageaccount
        - name: STORAGE_KEY
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: storage-key
        % elif cloud == "gcp" :
        - name:  GOOGLE_APPLICATION_CREDENTIALS
          value: /root/.gcp/serviceaccount.json
        % elif cloud == "openstack" or cloud == "os" :
        - name: OS_AUTH_URL
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: authURL
        - name: OS_DOMAIN_NAME
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: domainName
        - name: OS_USERNAME
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: username
        - name: OS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: password
        - name: OS_TENANT_NAME
          valueFrom:
            secretKeyRef:
              name: secret-${cloud}
              key: tenantName
        % endif
        volumeMounts:
        - mountPath: /var/etcd/data
          name: etcd-data-${cloud}
        % if cloud == "gcp" :
        - mountPath: /root/.gcp/
          name: secret-${cloud}
        % endif
      volumes:
        - name: etcd-bootstrap
          configMap:
            name: etcd-bootstrap-${cloud}
            defaultMode: 0356
        % if cloud != "local" :
        - name: secret-${cloud}
          secret:
            secretName: secret-${cloud} # change the secret name here
            defaultMode: 0420
        % endif
  volumeClaimTemplates:
  - metadata:
      name: etcd-data-${cloud}
    spec:
      accessModes:
      - "ReadWriteOnce"
      resources:
        requests:
          storage: 1Gi