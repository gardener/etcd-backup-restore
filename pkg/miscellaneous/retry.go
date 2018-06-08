// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package miscellaneous

import (
	"time"
)

// Do retries the retryFunc exponentially with backoff.
// It is mutated from `retry.Do` function in package
// [retry-go](https://github.com/avast/retry-go)
func Do(retryFunc func() error, config *Config) error {
	var err error
	config.Logger.Infof("Job attempt: %d", 1)
	err = retryFunc()
	if err == nil {
		return nil
	}
	for n := uint(1); n < config.Attempts; n++ {
		delayTime := config.Delay * (1 << (n - 1))
		time.Sleep((time.Duration)(delayTime) * config.Units)
		config.Logger.Infof("Job attempt: %d", n+1)
		err = retryFunc()
		if err == nil {
			return nil
		}
	}
	return err
}
