# Etcd data validation

 As etcd is being used to store the state of the K8s cluster, it is mandatory that etcd deployment has to be hardened against data loss. Sufficient checks have to be in place to prevent etcd from erroneously starting with stale/corrupt data and taking stale snapshots to the backing store. We have a data validation flow in place which prevents etcd from starting in case of data corruption. 	

 ## Directory validation	
The etcd data directory validation comprises of multiple checks as mentioned below:	
### Structure validation	
The member directory, snap directory and wal directory are checked to ascertain that they adhere to the directory structure followed by etcd.	
### Content validation	
#### Corruption check	
The contents for the data directory(db file, snap files and wal file) are checked for data corruption. 	
#### Revision check	
The revision of etcd data in the db file is checked with the revision of the latest snapshot in the backing store. If the revison in the backing store is greater than that of etcd data in the db file, etcd data is considered stale. This is to prevent etcd snapshots for stale revisions from overwriting legit recent snapshots.	

 ## Validation flow	
Not all validation steps take the same time to complete. Some validation steps are dependent on the size of etcd data(eg. db file). If the db file is checked for data corruption before etcd startup, it would take longer for etcd to become servicable. Therefore, it is only imperative to perform validation checks on abnormal etcd events like etcd restart after a crash. The validation flow mentioned below is modeled with the aforementioned rationale in mind.	

 * Is the validation marker file present?	
  * No	
    * Do directory structure validation.	
    * Do directory content validation.	
    * Start etcd	
  * Yes	
    * Check if previous exit was normal	from the validation marker file
      * Yes 	
        * Do revision check	
        * Do directory structure validation.	
        * Start etcd	
      * No  	
        * Do directory structure validation.	
        * Do directory content validation.	
        * Start etcd	

  ## Addition design decisions to be made	
 Currently, we have the validation check triggered from a bash script in the etcd container. The status of the validation check is polled till its completed and based on the validation status, it is decided whether it is safe to start etcd. During validation if etcd directory is found to be corrupt or stale, the latest snapshot in the backing store is used to restore etcd data to the latest revision. 	

 ### Question 1: Should the sidecar container be able to act on the status of previous etcd run status?	

 * **Option 1**: Yes. The information of previous etcd run may be made available to the sidecar container via configmaps. The idea is that `validate` REST endpoint shall check the shared configmap for status, perform necessary validation and restore steps before etcd start.	

 * **Option 2**: No. If the above-mentioned level of granularity is to be available for validation checks, we would need to modify the REST endpoints to trigger the validation sub-checks. Should we modify the bash script to handle the cases and let the sidecar be agnostic to the status of the previous etcd run?	

 We have chosen the approach were the script decides on the previous exit status of etcd, to call the necessary validation step. If etcd terminated normally then sanity validation is performed else we perform a full etcd data validation.

 ### Question 2: How should status for previous etcd run be identified?	
* **Option 1**: The error logs of the etcd run can be dumped to an log file in the persistent disk. This can be checked on subsequent validation steps to identify the status of previous etcd run.	
* **Option 2**: Via exit code stored in a file in the persistent disk. This can be checked on subsequent validation steps to identify the status of previous etcd run.

Since we are do not do an analysis of the logs at this point of time, the log dump and subsequent analysis steps can be taken care of in the necessary PR.