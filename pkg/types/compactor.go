package types

import (
	"fmt"
	"io/ioutil"
	"time"
)

// CompactionResult holds the compaction details after a compaction is done
type CompactionResult struct {
	// Snapshot               *snapstore.Snapshot
	Path                   string
	LastCompactionDuration time.Duration
}

// CompactionConfig holds the configuration data for compaction
type CompactionConfig struct {
	*RestorationConfig
}

// NewCompactionConfig returns the compaction config.
func NewCompactionConfig() (*CompactionConfig, error) {
	restoreDir, err := getEtcdDir("/tmp")
	if err != nil {
		return nil, err
	}

	return &CompactionConfig{
		&RestorationConfig{
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
			AutoCompactionMode:       defaultAutoCompactionMode,
			AutoCompactionRetention:  defaultAutoCompactionRetention,
		},
	}, nil
}

func getEtcdDir(dir string) (string, error) {
	outputDir, err := ioutil.TempDir(dir, "compactor")
	if err != nil {
		return "", err
	}

	return outputDir, nil
}
