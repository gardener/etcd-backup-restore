module github.com/gardener/etcd-backup-restore

go 1.14

require (
	cloud.google.com/go v0.50.0
	cloud.google.com/go/storage v1.0.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/MakeNowJust/heredoc v1.0.0 // indirect
	github.com/aliyun/aliyun-oss-go-sdk v2.1.8+incompatible
	github.com/aws/aws-sdk-go v1.32.6
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/elazarl/goproxy v0.0.0-20191011121108-aa519ddbe484 // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/gophercloud/gophercloud v0.7.0
	github.com/gophercloud/utils v0.0.0-20200204043447-9864b6f1f12f
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.2 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/onsi/ginkgo v1.14.0
	github.com/onsi/gomega v1.10.1
	github.com/prometheus/client_golang v1.7.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/tmc/grpc-websocket-proxy v0.0.0-20200427203606-3cfed13b9966 // indirect
	go.etcd.io/bbolt v1.3.3
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738
	go.opencensus.io v0.22.1 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.0.0-20200707034311-ab3426394381 // indirect
	golang.org/x/sys v0.0.0-20200728102440-3e129f6d46b1 // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	google.golang.org/api v0.14.0
	helm.sh/helm/v3 v3.2.4
	k8s.io/api v0.18.0
	k8s.io/apimachinery v0.18.0
	k8s.io/client-go v11.0.0+incompatible
	rsc.io/letsencrypt v0.0.3 // indirect
)

replace (
	// Ref: https://github.com/Azure/go-autorest/issues/414
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v13.3.2+incompatible
	// Etcd issue Ref: https://github.com/etcd-io/etcd/issues/12068
	// Etcd 3.4.x vendoring issue Ref: https://github.com/etcd-io/etcd/issues/11154#issuecomment-677940701
	github.com/coreos/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b // ae9734ed278b is the SHA for git tag v3.4.13
	github.com/docker/docker => github.com/moby/moby v0.7.3-0.20190826074503-38ab9da00309
	// Ref: https://github.com/etcd-io/etcd/issues/11992
	github.com/golang/protobuf => github.com/golang/protobuf v1.3.5
	// Ref: https://github.com/etcd-io/etcd/issues/11707
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
	k8s.io/api => k8s.io/api v0.18.0
	k8s.io/apimachinery => k8s.io/apimachinery v0.18.0
	k8s.io/client-go => k8s.io/client-go v0.18.0
)
