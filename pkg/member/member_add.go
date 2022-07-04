package member

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil/client"
	"github.com/gardener/etcd-backup-restore/pkg/miscellaneous"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"gopkg.in/yaml.v2"
	"k8s.io/client-go/util/retry"
)

const (
	// retryPeriod is the peroid after which an operation is retried
	retryPeriod = 5 * time.Second

	// etcdTimeout is timeout for etcd operations
	etcdTimeout = 10 * time.Second
)

// AddMember interface defines the functionalities needed to add a member to the etcd cluster
type AddMember interface {
	// AddMemberAsLearner add a new member as a learner to the etcd cluster
	AddMemberAsLearner(*logrus.Logger, *brtypes.EtcdConnectionConfig)

	// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster
	IsMemberInCluster(*logrus.Logger, *brtypes.EtcdConnectionConfig)

	// PromoteMember promotes an etcd member from a learner to a voting member of the cluster. This will succeed only if its logs are caught up with the leader
	PromoteMember(context.Context, *logrus.Entry, *brtypes.EtcdConnectionConfig)
}

// NewMember holds the configuration for the mechanism of adding a new member to the cluster.
type NewMember struct {
	clientFactory client.Factory
}

// NewMemberConfig returns new ExponentialBackoff.
func NewMemberConfig(etcdConnConfig *brtypes.EtcdConnectionConfig) *NewMember {
	etcdConn := *etcdConnConfig
	svcEndpoint, err := miscellaneous.GetEtcdSvcEndpoint()
	if svcEndpoint == "" || err != nil {
		svcEndpoint = "http://127.0.0.1:2379"
	}
	etcdConn.Endpoints = []string{svcEndpoint}
	clientFactory := etcdutil.NewFactory(etcdConn)
	return &NewMember{
		clientFactory: clientFactory,
	}
}

// AddMemberAsLearner add a member as a learner to the etcd cluster
func (m *NewMember) AddMemberAsLearner(logger *logrus.Logger) error {
	//Add member as learner to cluster
	logger.Info("Starting Add Member")
	memberURL, err := getMemberURL(logger)
	if memberURL == "" || err != nil {
		logger.Info("Error fetching etcd member URL")
		return fmt.Errorf("Could not fetch member URL : %v", err)
	}

	cli, err := m.clientFactory.NewCluster()
	defer cli.Close()
	if err != nil {
		logger.Info("Error creating etcd client to add member")
		return fmt.Errorf("failed to build etcd cluster client : %v", err)
	}

	memAddCtx, cancel := getContext(context.TODO(), etcdTimeout)
	defer cancel()
	_, err = cli.MemberAddAsLearner(memAddCtx, []string{memberURL})

	if err == nil {
		logger.Info("Added member to cluster as a learner")
		return nil
	}
	if errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCPeerURLExist)) {
		logger.Info("Member already part of etcd cluster")
		return nil
	}
	if err != nil {
		logger.Warn("Error adding member as a learner: ", err)
		return fmt.Errorf("Error adding member as a learner: %v", err)
	}

	logger.Info("Could not as member as learner due to: ", err)
	return fmt.Errorf("Could not as member as learner due to: %v", err)
}

// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster
func (m *NewMember) IsMemberInCluster(logger *logrus.Logger) (bool, error) {
	// Check if an etcd is already available
	backoff := retry.DefaultBackoff
	backoff.Steps = 2
	err := retry.OnError(backoff, func(err error) bool {
		return err != nil
	}, func() error {
		etcdProbeCtx, cancel := getContext(context.TODO(), etcdTimeout)
		defer cancel()
		return miscellaneous.ProbeEtcd(etcdProbeCtx, m.clientFactory, logger)
	})
	if err != nil {
		return false, err
	}

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		logger.Errorf("failed to build etcd cluster client")
		return false, err
	}
	defer cli.Close()
	logger.Info("Etcd client created")

	// List members in cluster
	var etcdMemberList *clientv3.MemberListResponse
	err = retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return err != nil
	}, func() error {
		memListCtx, cancel := getContext(context.TODO(), etcdTimeout)
		defer cancel()
		etcdMemberList, err = cli.MemberList(memListCtx)
		return err
	})
	if err != nil {
		logger.Warn("Could not list any etcd members", err)
		return false, err
	}

	podName, err := miscellaneous.GetEnvVarOrError("POD_NAME")
	if err != nil {
		logger.Error("Error reading POD_NAME env var", err)
		return false, err
	}
	for _, y := range etcdMemberList.Members {
		if y.Name == podName {
			return true, err
		}
	}

	return false, err
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
	}

	config := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(configYML), &config); err != nil {
		logger.Fatalf("Unable to unmarshal etcd config yaml file: %v", err)
	}

	initAdPeerURL := config["initial-advertise-peer-urls"]
	protocol, svcName, namespace, peerPort, err := parsePeerURL(fmt.Sprint(initAdPeerURL))
	podName := os.Getenv("POD_NAME")
	domaiName := fmt.Sprintf("%s.%s.%s", svcName, namespace, "svc")

	return fmt.Sprintf("%s://%s.%s:%s", protocol, podName, domaiName, peerPort), nil
}

func parsePeerURL(peerURL string) (string, string, string, string, error) {
	tokens := strings.Split(peerURL, "@")
	if len(tokens) < 4 {
		return "", "", "", "", fmt.Errorf("total length of tokens is less than four")
	}
	return tokens[0], tokens[1], tokens[2], tokens[3], nil
}

// UpdateMemberPeerAddress updated the peer address of a specified etcd member
func (m *NewMember) UpdateMemberPeerAddress(ctx context.Context, logger *logrus.Entry, id uint64) {
	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		logger.Errorf("failed to build etcd cluster client")
	}

	memberURL, _ := getMemberURL(logger.Logger)
	if memberURL == "" || err != nil {
		logger.Errorf("Could not fetch member URL : %v", err)
	}

	ctx, cancel := getContext(context.TODO(), etcdTimeout)
	defer cancel()

	cli.MemberUpdate(ctx, id, []string{memberURL})
}

// PromoteMember promotes an etcd member from a learner to a voting member of the cluster. This will succeed only if its logs are caught up with the leader
func (m *NewMember) PromoteMember(ctx context.Context, logger *logrus.Entry) error {
	logger.Info("In Member Promote")
	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		logger.Errorf("failed to build etcd cluster client")
	}

	//List all members in the etcd cluster
	//Member URL will appear in the memberlist call response as soon as the member has been added to the cluster as a learner
	//However, the name of the member will appear only if the member has started running
	memListCtx, memListCtxCancel := getContext(ctx, brtypes.DefaultEtcdConnectionTimeout)
	etcdList, memListErr := cli.MemberList(memListCtx)
	memListCtxCancel()

	if memListErr != nil {
		logger.Info("error listing members: ", memListErr)
		cli.Close()
		return memListErr
	}

	//TODO: Simplify logic below
	podName, err := miscellaneous.GetEnvVarOrError("POD_NAME")
	if err != nil {
		logger.Error("Error reading POD_NAME env var", err)
	}
	for _, y := range etcdList.Members {
		if y.Name == podName {
			var memPromoteErr error
			memPromoteCtx, cancel := getContext(ctx, brtypes.DefaultEtcdConnectionTimeout)
			defer cancel()
			_, memPromoteErr = cli.MemberPromote(memPromoteCtx, y.ID) //Member promote call will succeed only if member is in sync with leader, and will error out otherwise
			if memPromoteErr == nil {                                 //Member successfully promoted
				logger.Info("Member promoted ", y.Name, " : ", y.ID)
				return nil
			} else if errors.Is(memPromoteErr, rpctypes.Error(rpctypes.ErrGRPCMemberNotLearner)) { //Member is not a learner
				if y.PeerURLs[0] == []string{"http://localhost:2380"}[0] {
					// Already existing clusters have `http://localhost:2380` as the peer address. This needs to explicitly updated to the new address
					// TODO: Remove this peer address updation logic on etcd-br v0.20.0
					m.UpdateMemberPeerAddress(ctx, logger, y.ID)
				}
				logger.Info("Member ", y.Name, " : ", y.ID, " already part of etcd cluster")
				return nil
			}
			logger.Info("Could not promote member ", y.Name, " : ", memPromoteErr)
			err = memPromoteErr
			break
		}
	}
	logger.Info("Member still catching up logs from leader. Retrying promotion...")
	if err == nil {
		err = fmt.Errorf("Member not added to cluster yet : %v", err)
	}
	return err
}

func getContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctxt, cancel := context.WithTimeout(ctx, timeout)
	return ctxt, cancel
}
