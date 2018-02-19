Etcd backup restorer  
Copyright 2018 The Gardener Authors.  
[Apache 2 license](./LICENSE.md ).

## Seed Source

The source code of this component was seeded based on a copy of the following files from [github.com/coreos/etcd](github.com/coreos):

Etcd.  
https://github.com/coreos/etcd.  
Copyright 2017 The etcd Authors.  
Apache 2 license (https://github.com/coreos/etcd/blob/v3.3.1/LICENSE).

Release: v3.3.1.  
Commit-ID: 28f3f26c0e303392556035b694f75768d449d33d.  
Commit-Message: version: 3.3.1.  
To the left are the list of copied files -> and to the right the current location they are at.  
etcdctl/ctlv3/command/snapshot_command.go ->  pkg/restorer/restorer.go

## Dependencies

The etcd-backup-restorer includes the following components

Cobra.  
https://github.com/spf13/cobra.  
Copyright 2015 Steve Francia.  
Apache 2 license (https://github.com/spf13/cobra/blob/master/LICENSE.txt)

Etcd.  
https://github.com/coreos/etcd.  
Copyright 2017 The etcd Authors.  
Apache 2 license (https://github.com/coreos/etcd/blob/master/LICENSE).

Cron.  
https://github.com/robfig/cron.  
Copyright (C) 2012 Rob Figueiredo.  
MIT license(https://github.com/robfig/cron/blob/master/LICENSE)

Logrus.  
https://github.com/sirupsen/logrus.  
Copyright (c) 2014 Simon Eskildsen.  
MIT license (https://github.com/sirupsen/logrus/blob/master/LICENSE)

AWS SDK for Go.  
https://github.com/aws/aws-sdk-go.  
Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.  
Apache 2 license(https://github.com/aws/aws-sdk-go/blob/master/LICENSE.txt)