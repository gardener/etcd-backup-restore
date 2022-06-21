package member

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"gopkg.in/yaml.v2"
)

const (
	// RetryPeriod is the peroid after which an operation is retried
	RetryPeriod time.Duration = 5 * time.Second
)

// AddMemberAsLearner add a member as a learner to the etcd cluster
func AddMemberAsLearner(logger *logrus.Logger, etcdConnConfig *brtypes.EtcdConnectionConfig) error {
	//Add member as learner to cluster
	memberURL, err := getMemberURL(logger)
	if memberURL == "" || err != nil {
		logger.Warn("Could not fetch member URL")
	}
	for {
		//Create etcd client
		etcdConn := *etcdConnConfig
		etcdConn.Endpoints = []string{fmt.Sprintf("%s://%s:%s", "https", "etcd-main-0.etcd-main-peer.default.svc", "2379")} //TODO: Don't hardcode this
		clientFactory := etcdutil.NewFactory(etcdConn)
		cli, _ := clientFactory.NewCluster()

		memAddCtx, cancel := context.WithTimeout(context.TODO(), brtypes.DefaultEtcdConnectionTimeout)
		_, err := cli.MemberAddAsLearner(memAddCtx, []string{memberURL})
		cancel()
		cli.Close()

		if err != nil {
			logger.Warn("Error adding member as a learner: ", err)
		}
		if err == nil || strings.Contains(rpctypes.ErrGRPCPeerURLExist.Error(), err.Error()) {
			logger.Info("Added member to cluster as a learner")
			break //TODO: why not just return here?
		}
		if strings.Contains(rpctypes.ErrGRPCPeerURLExist.Error(), err.Error()) {
			logger.Info("Member already part of etcd cluster")
			break
		}

		logger.Info("Could not as member as learner due to: ", err)
		logger.Info("Trying again in 5 seconds... ")
		timer := time.NewTimer(RetryPeriod)
		<-timer.C
		timer.Stop()
	}

	return nil
}

// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster
func IsMemberInCluster(logger *logrus.Logger, etcdConnConfig *brtypes.EtcdConnectionConfig) bool {
	//Create etcd client
	etcdConn := *etcdConnConfig
	etcdConn.Endpoints = []string{fmt.Sprintf("%s://%s:%s", "https", "etcd-main-0.etcd-main-peer.default.svc", "2379")} //TODO: Don't hardcode this
	clientFactory := etcdutil.NewFactory(etcdConn)
	cli, _ := clientFactory.NewCluster()

	// TODO: should use a retry mechanism here
	defer cli.Close()
	logger.Info("Etcd client created")

	// List members in cluster
	memListCtx, cancel := context.WithTimeout(context.TODO(), brtypes.DefaultEtcdConnectionTimeout)
	etcdMemberList, err := cli.MemberList(memListCtx)
	defer cancel()
	if err != nil {
		logger.Warn("Could not list any etcd members", err)
		return true
	}

	for _, y := range etcdMemberList.Members {
		if y.Name == os.Getenv("POD_NAME") {
			return true
		}
	}

	return false
}

func getMemberURL(logger *logrus.Logger) (string, error) {
	var inputFileName string
	etcdConfigForTest := os.Getenv("ETCD_CONF")
	if etcdConfigForTest != "" {
		inputFileName = etcdConfigForTest
	} else {
		inputFileName = "/var/etcd/config/etcd.conf.yaml"
	}

	configYML, err := os.ReadFile(inputFileName)
	if err != nil {
		logger.Fatalf("Unable to read etcd config file: %v", err)
		//return "", err
	}

	config := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(configYML), &config); err != nil {
		logger.Warnf("Unable to unmarshal etcd config yaml file: %v", err)
		return "", err
	}

	return strings.Split(fmt.Sprint(config["initial-advertise-peer-urls"]), ",")[0], nil //TODO: too complicated
}

// PromoteMember promotes an etcd member from a learner to a voting member of the cluster. This will succeed only if its logs are caught up with the leader
func PromoteMember(ctx context.Context, logger *logrus.Entry, etcdConnConfig *brtypes.EtcdConnectionConfig) {
	for {
		etcdConn := *etcdConnConfig
		//etcdConn.Endpoints = []string{os.Getenv("ETCD_ENDPOINT")}
		etcdConn.Endpoints = []string{fmt.Sprintf("%s://%s:%s", "https", "etcd-main-0.etcd-main-peer.default.svc", "2379")} //TODO: Don't hardcode this

		clientFactory := etcdutil.NewFactory(etcdConn)
		cli, _ := clientFactory.NewCluster()

		//List all members in the etcd cluster
		//Member URL will appear in the memberlist call response as soon as the member has been added to the cluster as a learner
		//However, the name of the member will appear only if the member has started running
		memListCtx, memListCtxcancel := context.WithTimeout(context.TODO(), brtypes.DefaultEtcdConnectionTimeout)
		etcdList, memListErr := cli.MemberList(memListCtx)
		memListCtxcancel()

		if memListErr != nil {
			logger.Info("error listing members: ", memListErr)
			cli.Close()
			continue
		}

		//TODO: Simplify logic below
		var promoted bool
		promoted = false
		for _, y := range etcdList.Members {
			if y.Name == os.Getenv("POD_NAME") {
				logger.Info("Promoting member ", y.Name)
				memPromoteCtx, cancel := context.WithTimeout(context.TODO(), brtypes.DefaultEtcdConnectionTimeout)
				//Member promote call will succeed only if member is in sync with leader, and will error out otherwise
				_, memPromoteErr := cli.MemberPromote(memPromoteCtx, y.ID)
				cancel()
				if memPromoteErr == nil {
					//Exit if member is successfully promoted or if member is not a learner
					promoted = true
					logger.Info("Member promoted ", y.Name, " : ", y.ID)
					break
				}
				if strings.Contains(rpctypes.ErrGRPCMemberNotLearner.Error(), memPromoteErr.Error()) {
					//Exit if member is already part of the cluster
					promoted = true
					logger.Info("Member ", y.Name, " : ", y.ID, " already part of etcd cluster")
					break
				}
			}
		}
		if promoted {
			break
		}

		//Timer here so that the member promote loop doesn't execute too frequently
		logger.Info("Member still catching up logs from leader. Retrying promotion...")
		timer := time.NewTimer(RetryPeriod)
		<-timer.C
		timer.Stop()
	}
}
