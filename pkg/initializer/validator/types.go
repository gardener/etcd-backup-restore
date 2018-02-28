// Copyright © 2018 The Gardener Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package validator

import "github.com/sirupsen/logrus"

// Config store configuration for DataValidator.
type Config struct {
	DataDir string
}

// DataValidator contains implements Validator interface to perform data validation.
type DataValidator struct {
	Config *Config
	Logger *logrus.Logger
}

// Validator is the interface for data validation actions.
type Validator interface {
	Validate() error
}
