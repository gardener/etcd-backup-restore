package initializer

import (
	"github.com/gardener/etcd-backup-restore/pkg/initializer/validator"
	"github.com/sirupsen/logrus"
)

type InitializerConfig struct {
	DataDir         string
	StorageProvider string
}

type EtcdInitializer struct {
	Validator *validator.DataValidator
	Config    *InitializerConfig
	Logger    *logrus.Logger
}

type Initializer interface {
	Initialize() error
}
