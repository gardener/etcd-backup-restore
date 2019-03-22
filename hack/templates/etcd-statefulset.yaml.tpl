<%
  import os

  if context.get("cloud", "") == "":
    raise Exception("missing --var cloud={aws azure gcp openstack local} flag")

  if context.get("container", "") == "":
   raise Exception("missing --var container=ContainerName flag")  
  provider=""
  imageTag="0.5.0"
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
    VALIDATION_MARKER=/var/etcd/data/validation_marker
    
    trap_and_propagate() {
        PID=$1
        shift
        for sig in "$@" ; do
            trap "kill -$sig $PID" "$sig"
        done
    }
    
    start_managed_etcd(){
          etcd --config-file /bootstrap/etcd.conf.yml &
          ETCDPID=$!
          trap_and_propagate $ETCDPID INT TERM
          wait $ETCDPID
          RET=$?
          echo $RET > $VALIDATION_MARKER
          exit $RET
    }
    
    check_and_start_etcd(){
          while true;
          do
            wget http://localhost:8080/initialization/status -S -O status;
            STATUS=`cat status`;
            case $STATUS in
            "New")
                  wget http://localhost:8080/initialization/start?mode=$1 -S -O - ;;
            "Progress")
                  sleep 1;
                  continue;;
            "Failed")
                  continue;;
            "Successful")
                  start_managed_etcd
                  break
                  ;;
            esac;
          done
    }
    
    if [ ! -f $VALIDATION_MARKER ] ;
    then 
          echo "No VALIDATION_MARKER file. Perform complete initialization routine and start etcd."
          check_and_start_etcd full
    else
          echo "VALIDATION_MARKER file present. Check return status and decide on initialization"
          run_status=`cat $VALIDATION_MARKER`
          echo "VALIDATION_MARKER content: $run_status"
          if [ $run_status == '143' ] || [ $run_status == '130' ] || [ $run_status == '0' ] ; then
                rm -rf $VALIDATION_MARKER
                echo "Requesting sidecar to perform sanity validation"
                check_and_start_etcd sanity
          else
                rm -rf $VALIDATION_MARKER
                echo "Requesting sidecar to perform full validation"
                check_and_start_etcd full
          fi    
    fi
  etcd.conf.yml: |-
      # This is the configuration file for the etcd server.

      # Human-readable name for this member.
      name: etcd-new

      # Path to the data directory.
      data-dir: /var/etcd/data/new.etcd

      # List of this member's client URLs to advertise to the public.
      # The URLs needed to be a comma-separated list.
      advertise-client-urls: http://0.0.0.0:2379

      # List of comma separated URLs to listen on for client traffic.
      listen-client-urls: http://0.0.0.0:2379

      # Initial cluster token for the etcd cluster during bootstrap.
      initial-cluster-token: 'new'

      # Initial cluster state ('new' or 'existing').
      initial-cluster-state: 'new'

      # Number of committed transactions to trigger a snapshot to disk.
      snapshot-count: 75000  

      # Raise alarms when backend size exceeds the given quota. 0 means use the
      # default quota.
      quota-backend-bytes: 8589934592

      # Accept etcd V2 client requests
      enable-v2: false

      # keep one day of history
      auto-compaction-mode: periodic
      auto-compaction-retention: "24"
  
    
    
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
        image: quay.io/coreos/etcd:v3.3.10
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
        - --schedule=* */1 * * *
        - --data-dir=/var/etcd/data/new.etcd
        - --insecure-transport=true
        - --storage-provider=${provider}
        - --delta-snapshot-period-seconds=3600
        - --garbage-collection-period-seconds=1800
        - --snapstore-temp-directory=/var/etcd/data/temp
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
