module github.com/gardener/etcd-backup-restore

go 1.12

require (
	cloud.google.com/go v0.46.3
	cloud.google.com/go/storage v1.0.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/aliyun/aliyun-oss-go-sdk v2.0.3+incompatible
	github.com/aws/aws-sdk-go v1.24.5
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/coreos/bbolt v1.3.3
	github.com/coreos/etcd v3.3.15+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/gogo/protobuf v1.3.0 // indirect
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/google/uuid v1.1.1 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gophercloud/gophercloud v0.4.0
	github.com/gophercloud/utils v0.0.0-20190829151529-94e6842399e5
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.2 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/onsi/ginkgo v1.10.1
	github.com/onsi/gomega v1.7.0
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.7.0 // indirect
	github.com/prometheus/procfs v0.0.5 // indirect
	github.com/robfig/cron/v3 v3.0.0
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.opencensus.io v0.22.1 // indirect
	golang.org/x/crypto v0.0.0-20190923035154-9ee001bba392 // indirect
	golang.org/x/exp v0.0.0-20190919035709-81c71964d733 // indirect
	golang.org/x/net v0.0.0-20190923162816-aa69164e4478 // indirect
	golang.org/x/sys v0.0.0-20190924154521-2837fb4f24fe // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	golang.org/x/tools v0.0.0-20190925020647-22afafe3322a // indirect
	google.golang.org/api v0.10.0
	google.golang.org/appengine v1.6.3 // indirect
	google.golang.org/genproto v0.0.0-20190916214212-f660b8655731 // indirect
	google.golang.org/grpc v1.23.1 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.0.0-20190831074750-7364b6bdad65
	k8s.io/apimachinery v0.0.0-20190831074630-461753078381
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/klog v0.4.0 // indirect
	k8s.io/utils v0.0.0-20190829053155-3a4a5477acf8 // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.0.0-20190819141258-3544db3b9e44 // k8s 1.15.3
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190817020851-f2f3a405f61d //k8s 1.15.3
	k8s.io/client-go => k8s.io/client-go v0.0.0-20190819141724-e14f31a72a77 //k8s 1.15.3
)
