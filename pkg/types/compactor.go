package types

import (
	"fmt"
	"io/ioutil"
	"path"
	"time"

	flag "github.com/spf13/pflag"
	"go.etcd.io/etcd/pkg/types"
)

// CompactionResult holds the compaction details after a compaction is done
type CompactionResult struct {
	// Snapshot               *snapstore.Snapshot
	Path                   string
	LastCompactionDuration time.Duration
}

// CompactionConfig holds the configuration data for compaction
type CompactionConfig struct {
	rc *RestorationConfig
}

// NewCompactionConfig returns the compaction config.
func NewCompactionConfig() (*CompactionConfig, error) {
	restoreDir, err := getEtcdDir("/tmp")
	if err != nil {
		return nil, err
	}

	return &CompactionConfig{
		rc: &RestorationConfig{
			InitialCluster:           initialClusterFromName(defaultName),
			InitialClusterToken:      defaultInitialClusterToken,
			RestoreDataDir:           fmt.Sprintf("%s/%s.etcd", restoreDir, defaultName),
			InitialAdvertisePeerURLs: []string{defaultInitialAdvertisePeerURLs},
			Name:                     defaultName,
			SkipHashCheck:            false,
			MaxFetchers:              defaultMaxFetchers,
			MaxCallSendMsgSize:       defaultMaxCallSendMsgSize,
			MaxRequestBytes:          defaultMaxRequestBytes,
			MaxTxnOps:                defaultMaxTxnOps,
			EmbeddedEtcdQuotaBytes:   int64(defaultEmbeddedEtcdQuotaBytes),
		},
	}, nil
}

// AddFlags adds the flags to flagset.
func (c *CompactionConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.rc.InitialCluster, "initial-cluster", c.rc.InitialCluster, "initial cluster configuration for restore bootstrap")
	fs.StringVar(&c.rc.InitialClusterToken, "initial-cluster-token", c.rc.InitialClusterToken, "initial cluster token for the etcd cluster during restore bootstrap")
	fs.StringVarP(&c.rc.RestoreDataDir, "data-dir", "d", c.rc.RestoreDataDir, "path to the data directory")
	fs.StringArrayVar(&c.rc.InitialAdvertisePeerURLs, "initial-advertise-peer-urls", c.rc.InitialAdvertisePeerURLs, "list of this member's peer URLs to advertise to the rest of the cluster")
	fs.StringVar(&c.rc.Name, "name", c.rc.Name, "human-readable name for this member")
	fs.BoolVar(&c.rc.SkipHashCheck, "skip-hash-check", c.rc.SkipHashCheck, "ignore snapshot integrity hash value (required if copied from data directory)")
	fs.UintVar(&c.rc.MaxFetchers, "max-fetchers", c.rc.MaxFetchers, "maximum number of threads that will fetch delta snapshots in parallel")
	fs.IntVar(&c.rc.MaxCallSendMsgSize, "max-call-send-message-size", c.rc.MaxCallSendMsgSize, "maximum size of message that the client sends")
	fs.UintVar(&c.rc.MaxRequestBytes, "max-request-bytes", c.rc.MaxRequestBytes, "Maximum client request size in bytes the server will accept")
	fs.UintVar(&c.rc.MaxTxnOps, "max-txn-ops", c.rc.MaxTxnOps, "Maximum number of operations permitted in a transaction")
	fs.Int64Var(&c.rc.EmbeddedEtcdQuotaBytes, "embedded-etcd-quota-bytes", c.rc.EmbeddedEtcdQuotaBytes, "maximum backend quota for the embedded etcd used for applying delta snapshots")
}

// Validate validates the config.
func (c *CompactionConfig) Validate() error {
	if _, err := types.NewURLsMap(c.rc.InitialCluster); err != nil {
		return fmt.Errorf("failed creating url map for restore cluster: %v", err)
	}
	if _, err := types.NewURLs(c.rc.InitialAdvertisePeerURLs); err != nil {
		return fmt.Errorf("failed parsing peers urls for restore cluster: %v", err)
	}
	if c.rc.MaxCallSendMsgSize <= 0 {
		return fmt.Errorf("max call send message should be greater than zero")
	}
	if c.rc.MaxFetchers <= 0 {
		return fmt.Errorf("max fetchers should be greater than zero")
	}
	if c.rc.EmbeddedEtcdQuotaBytes <= 0 {
		return fmt.Errorf("Etcd Quota size for etcd must be greater than 0")
	}
	c.rc.RestoreDataDir = path.Clean(c.rc.RestoreDataDir)
	return nil
}

func getEtcdDir(dir string) (string, error) {
	outputDir, err := ioutil.TempDir(dir, "compactor")
	if err != nil {
		return "", err
	}

	return outputDir, nil
}
