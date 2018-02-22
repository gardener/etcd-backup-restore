package validator

import "github.com/sirupsen/logrus"

type ValidatorConfig struct {
	DataDir string
}

type DataValidator struct {
	Config *ValidatorConfig
	Logger *logrus.Logger
}

type Validator interface {
	Validate() error
}
