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
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestDO(t *testing.T) {
	config := &Config{
		Attempts: 4,
		Delay:    time.Duration(1),
		Units:    time.Duration(time.Second),
		Logger:   logrus.New(),
	}

	badRetryFunc := func() error {
		return fmt.Errorf("I'm bad func")
	}
	if err := Do(badRetryFunc, config); err == nil {
		t.Fatal(err)
	}
	goodRetryFunc := func() error {
		return nil
	}
	if err := Do(goodRetryFunc, config); err != nil {
		t.Fatal(err)
	}
}
