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
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"gopkg.in/yaml.v2"
	"k8s.io/client-go/util/retry"
)

const (
	// RetryPeriod is the peroid after which an operation is retried
	RetryPeriod = 2 * time.Second

	// EtcdTimeout is timeout for etcd operations
	EtcdTimeout = 5 * time.Second
)

var (
	// ErrMissingMember is a sentient error to describe a case of a member being missing from the member list
	ErrMissingMember = errors.New("member missing from member list")
)

// ControlMember interface defines the functionalities needed to manipulate a member in the etcd cluster
type ControlMember interface {
	// AddMemberAsLearner add a new member as a learner to the etcd cluster
	AddMemberAsLearner(context.Context) error

	// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster
	IsMemberInCluster(context.Context) (bool, error)

	// PromoteMember promotes an etcd member from a learner to a voting member of the cluster. This will succeed if and only if learner is in a healthy state and the learner is in sync with leader
	PromoteMember(context.Context) error

	// UpdateMember updates the peer address of a specified etcd cluster member.
	UpdateMember(context.Context, client.ClusterCloser) error
}

// memberControl holds the configuration for the mechanism of adding a new member to the cluster.
type memberControl struct {
	clientFactory client.Factory
	logger        logrus.Entry
	podName       string
	configFile    string
}

// NewMemberControl returns new ExponentialBackoff.
func NewMemberControl(etcdConnConfig *brtypes.EtcdConnectionConfig) ControlMember {
	var configFile string
	logger := logrus.New().WithField("actor", "member-add")
	etcdConn := *etcdConnConfig
	svcEndpoint, err := miscellaneous.GetEtcdSvcEndpoint()
	if err != nil {
		logger.Errorf("Error getting etcd service endpoint %v", err)
	}
	if svcEndpoint != "" {
		etcdConn.Endpoints = []string{svcEndpoint}
	}
	clientFactory := etcdutil.NewFactory(etcdConn)
	podName, err := miscellaneous.GetEnvVarOrError("POD_NAME")
	if err != nil {
		logger.Fatalf("Error reading POD_NAME env var : %v", err)
	}
	//TODO: Refactor needed
	configFile = miscellaneous.GetConfigFilePath()

	return &memberControl{
		clientFactory: clientFactory,
		logger:        *logger,
		podName:       podName,
		configFile:    configFile,
	}
}

// AddMemberAsLearner add a member as a learner to the etcd cluster
func (m *memberControl) AddMemberAsLearner(ctx context.Context) error {
	//Add member as learner to cluster
	memberURL, err := getMemberURL(m.configFile, m.podName)
	if err != nil {
		m.logger.Fatalf("Error fetching etcd member URL : %v", err)
	}

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return fmt.Errorf("failed to build etcd cluster client : %v", err)
	}
	defer cli.Close()

	memAddCtx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()
	_, err = cli.MemberAddAsLearner(memAddCtx, []string{memberURL})

	if err != nil {
		if errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCPeerURLExist)) || errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCMemberExist)) {
			m.logger.Infof("Member %s already part of etcd cluster", memberURL)
			return nil
		}
		return fmt.Errorf("Error adding member as a learner: %v", err)
	}

	m.logger.Infof("Added member %s to cluster as a learner", memberURL)
	return nil
}

// IsMemberInCluster checks is the current members peer URL is already part of the etcd cluster
func (m *memberControl) IsMemberInCluster(ctx context.Context) (bool, error) {
	m.logger.Infof("Checking if member %s is part of a running cluster", m.podName)
	// Check if an etcd is already available
	backoff := retry.DefaultBackoff
	backoff.Steps = 2
	err := retry.OnError(backoff, func(err error) bool {
		return err != nil
	}, func() error {
		etcdProbeCtx, cancel := context.WithTimeout(context.TODO(), EtcdTimeout)
		defer cancel()
		return miscellaneous.ProbeEtcd(etcdProbeCtx, m.clientFactory, &m.logger)
	})
	if err != nil {
		return false, err
	}

	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		m.logger.Errorf("failed to build etcd cluster client")
		return false, err
	}
	defer cli.Close()

	// List members in cluster
	var etcdMemberList *clientv3.MemberListResponse
	err = retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return err != nil
	}, func() error {
		memListCtx, cancel := context.WithTimeout(context.TODO(), EtcdTimeout)
		defer cancel()
		etcdMemberList, err = cli.MemberList(memListCtx)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("could not list any etcd members %w", err)
	}

	for _, y := range etcdMemberList.Members {
		if y.Name == m.podName {
			m.logger.Infof("Member %s part of running cluster", m.podName)
			return true, nil
		}
	}

	m.logger.Infof("Member %v not part of any running cluster", m.podName)
	m.logger.Infof("Could not find member %v in the list", m.podName)
	return false, nil
}

func getMemberURL(configFile string, podName string) (string, error) {
	configYML, err := os.ReadFile(configFile)
	if err != nil {
		return "", fmt.Errorf("unable to read etcd config file: %v", err)
	}

	config := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(configYML), &config); err != nil {
		return "", fmt.Errorf("unable to unmarshal etcd config yaml file: %v", err)
	}

	initAdPeerURL := config["initial-advertise-peer-urls"]
	peerURL, err := parsePeerURL(initAdPeerURL.(string), podName)
	if err != nil {
		return "", fmt.Errorf("could not parse peer URL from the config file : %v", err)
	}
	return peerURL, nil
}

func parsePeerURL(peerURL, podName string) (string, error) {
	tokens := strings.Split(peerURL, "@")
	if len(tokens) < 4 {
		return "", fmt.Errorf("invalid peer URL : %s", peerURL)
	}
	domaiName := fmt.Sprintf("%s.%s.%s", tokens[1], tokens[2], "svc")

	return fmt.Sprintf("%s://%s.%s:%s", tokens[0], podName, domaiName, tokens[3]), nil
}

// updateMemberPeerAddress updated the peer address of a specified etcd member
func (m *memberControl) updateMemberPeerAddress(ctx context.Context, cli client.ClusterCloser, id uint64) error {
	m.logger.Infof("Updating member peer URL for %s", m.podName)

	memberURL, err := getMemberURL(m.configFile, m.podName)
	if err != nil {
		return fmt.Errorf("could not fetch member URL : %v", err)
	}

	memberUpdateCtx, cancel := context.WithTimeout(ctx, EtcdTimeout)
	defer cancel()

	if _, err := cli.MemberUpdate(memberUpdateCtx, id, []string{memberURL}); err == nil {
		m.logger.Info("Successfully updated the member peer URL")
		return nil
	}
	return err
}

// PromoteMember promotes an etcd member from a learner to a voting member of the cluster. This will succeed only if its logs are caught up with the leader
func (m *memberControl) PromoteMember(ctx context.Context) error {
	m.logger.Infof("Attempting to promote member %s", m.podName)
	cli, err := m.clientFactory.NewCluster()
	if err != nil {
		return fmt.Errorf("failed to build etcd cluster client : %v", err)
	}
	defer cli.Close()

	//List all members in the etcd cluster
	//Member URL will appear in the memberlist call response as soon as the member has been added to the cluster as a learner
	//However, the name of the member will appear only if the member has started running
	memListCtx, memListCtxCancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	etcdList, err := cli.MemberList(memListCtx)
	defer memListCtxCancel()

	if err != nil {
		return fmt.Errorf("error listing members: %v", err)
	}

	foundMember := findMember(etcdList.Members, m.podName)
	if foundMember == nil {
		return ErrMissingMember
	}

	return m.doPromoteMember(ctx, foundMember, cli)
}

func findMember(existingMembers []*etcdserverpb.Member, memberName string) *etcdserverpb.Member {
	for _, member := range existingMembers {
		if member.GetName() == memberName {
			return member
		}
	}
	return nil
}

// UpdateMember updates the peer address of a specified etcd cluster member.
func (m *memberControl) UpdateMember(ctx context.Context, cli client.ClusterCloser) error {
	m.logger.Infof("Attempting to update the member Info: %v", m.podName)
	ctx, cancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer cancel()

	membersInfo, err := cli.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("error listing members: %v", err)
	}

	return m.updateMemberPeerAddress(ctx, cli, membersInfo.Header.GetMemberId())
}

func (m *memberControl) doPromoteMember(ctx context.Context, member *etcdserverpb.Member, cli client.ClusterCloser) error {
	memPromoteCtx, cancel := context.WithTimeout(ctx, brtypes.DefaultEtcdConnectionTimeout)
	defer cancel()
	_, err := cli.MemberPromote(memPromoteCtx, member.ID) //Member promote call will succeed only if member is in sync with leader, and will error out otherwise
	if err == nil {                                       //Member successfully promoted
		m.logger.Infof("Member %v with ID: %v has been promoted", member.GetName(), member.GetID())
		return nil
	} else if errors.Is(err, rpctypes.Error(rpctypes.ErrGRPCMemberNotLearner)) { //Member is not a learner
		if member.PeerURLs[0] == "http://localhost:2380" { //[]string{"http://localhost:2380"}[0] {
			// Already existing clusters have `http://localhost:2380` as the peer address. This needs to explicitly updated to the new address
			// TODO: Remove this peer address updation logic on etcd-br v0.20.0
			if err := m.updateMemberPeerAddress(ctx, cli, member.ID); err != nil {
				m.logger.Errorf("Could not update member peer URL : %v", err)
			}
		}
		m.logger.Info("Member ", member.Name, " : ", member.ID, " already part of etcd cluster")
		return nil
	}
	return err
}
