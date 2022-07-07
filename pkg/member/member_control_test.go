package member_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/member"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Membercontrol", func() {
	var (
		etcdConnectionConfig *brtypes.EtcdConnectionConfig
	)

	BeforeEach(func() {
		etcdConnectionConfig = brtypes.NewEtcdConnectionConfig()
		etcdConnectionConfig.Endpoints = []string{etcd.Clients[0].Addr().String()}
		etcdConnectionConfig.ConnectionTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.SnapshotTimeout.Duration = 30 * time.Second
		etcdConnectionConfig.DefragTimeout.Duration = 30 * time.Second

		os.Setenv("POD_NAME", "test-pod")
	})

	AfterEach(func() {
		os.Unsetenv("POD_NAME")
	})

	Describe("Creating NewMemberControl", func() {
		It("should not return error with valid configuration", func() {
			ctrlMember := member.NewMemberControl(etcdConnectionConfig)
			Expect(ctrlMember).ShouldNot(BeNil())
		})
	})

	Describe("While attempting to add a new member as a learner", func() {
		BeforeEach(func() {
			os.Setenv("POD_NAME", "test-pod")
			outfile := "/tmp/etcd.conf.yaml"
			etcdConfigYaml := `# Human-readable name for this member.
    name: etcd1
    data-dir: ` + os.Getenv("ETCD_DATA_DIR") + `
    metrics: extensive
    snapshot-count: 75000
    enable-v2: false
    quota-backend-bytes: 1073741824
    listen-client-urls: http://0.0.0.0:2379
    advertise-client-urls: http://0.0.0.0:2379
    initial-advertise-peer-urls: http@etcd-main-peer@default@2380
    initial-cluster: etcd1=http://0.0.0.0:2380
    initial-cluster-token: new
    initial-cluster-state: new
    auto-compaction-mode: periodic
    auto-compaction-retention: 30m`

			err := os.WriteFile(outfile, []byte(etcdConfigYaml), 0755)
			Expect(err).ShouldNot(HaveOccurred())
			os.Setenv("ETCD_CONF", outfile)
		})
		AfterEach(func() {
			os.Unsetenv("ETCD_CONF")
		})
		Context("Member is not already part of the cluster", func() {
			It("Should add member to the cluster as a learner", func() {
				mem := member.NewMemberControl(etcdConnectionConfig)
				err := mem.AddMemberAsLearner(context.TODO())
				Expect(err).To(BeNil())
			})
		})
	})

	Describe("While attempting to check if etcd is part of a cluster", func() {
		BeforeEach(func() {
			os.Setenv("POD_NAME", "test-pod")
		})
		Context("When cluster is up and member is not part of the list", func() {
			It("Should return an error", func() {
				mem := member.NewMemberControl(etcdConnectionConfig)
				b, err := mem.IsMemberInCluster(context.TODO())
				fmt.Println("Error os : ", err, " -> ", b)
				Expect(err).ToNot(BeNil())
			})
		})
	})

})
