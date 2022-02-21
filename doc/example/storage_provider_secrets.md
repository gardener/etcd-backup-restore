
## Examples of secrets for storage providers:

### Azure Blob storage secret
 - ```yaml
      apiVersion: v1
      kind: Secret
      metadata:
        name: etcd-backup
      type: Opaque
      data: 
        storageAccount: ...
        storageKey: ...
    ```
- JSON format 
  ```yaml
     apiVersion: v1
     kind: Secret
     metadata:
       name: etcd-backup
     type: Opaque
     stringData:
       secret.json: |-
         {
         "storageAccount": "..." ,
         "storageKey": "..."
         } 
  ```

### AWS S3 secret:
  - ```yaml
       apiVersion: v1
       kind: Secret
       metadata:
         name: etcd-backup
       type: Opaque
       data:
         accessKeyID: ...
         region: ...
         secretAccessKey: ...
    ```
 - JSON format
   ```yaml
      apiVersion: v1
      kind: Secret
      metadata:
        name: etcd-backup
      type: Opaque
      stringData:
        secret.json: |-
          {
          "accessKeyID": "...",
          "region": "...",
          "secretAccessKey": "..."
          } 
    ``` 

### Openstack swift:
  - ```yaml
       apiVersion: v1
       kind: Secret
       metadata:
         name: etcd-backup
       type: Opaque
       data:
         authURL: ...
         domainName: ...
         password: ...
         region: 
         tenantName: ...
         username: ...
    ```
  - JSON format
     ```yaml
         apiVersion: v1
         kind: Secret
         metadata:
           name: etcd-backup
         type: Opaque
         stringData:
           secret.json: |-
             {
             "authURL": "...",
             "domainName": "...",
             "password": "...",
             "region": "...",
             "tenantName": "...",
             "username": "..."
             } 
        ``` 

### AliCloud Object storage:
  - ```yaml
       apiVersion: v1
       kind: Secret
       metadata:
         name: etcd-backup
       type: Opaque
       data:
         accessKeyID: ...
         accessKeySecret: ...
         storageEndpoint: ...
    ```
  - JSON format
     ```yaml
         apiVersion: v1
         kind: Secret
         metadata:
           name: etcd-backup
         type: Opaque
         stringData:
           secret.json: |-
             {
             "accessKeyID": "...",
             "accessKeySecret": "...",
             "storageEndpoint": "..."
             }
     ```          

### Google cloud storage:
 - ```yaml
     apiVersion: v1
     kind: Secret
     metadata:
       name: virtual-garden-etcd-main-backup
     type: Opaque
     data:
       serviceaccount.json: ...    
   ```