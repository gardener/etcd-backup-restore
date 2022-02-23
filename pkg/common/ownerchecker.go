// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// Checker checks if a certain condition is true.
type Checker interface {
	// Check checks that a certain condition is true.
	Check(ctx context.Context) (bool, error)
}

// NewOwnerChecker returns a Checker that checks if the given owner domain name resolves to the given owner ID,
// using the given resolver and logger.
func NewOwnerChecker(ownerName, ownerID string, timeout time.Duration, resolver Resolver, logger *logrus.Entry, failureThreshold uint) Checker {
	return &ownerChecker{
		ownerName:     ownerName,
		ownerID:       ownerID,
		timeout:       timeout,
		resolver:      resolver,
		logger:        logger,
		failThreshold: failureThreshold,
	}
}

type ownerChecker struct {
	ownerName     string
	ownerID       string
	timeout       time.Duration
	resolver      Resolver
	logger        *logrus.Entry
	failThreshold uint
}

// Check returns true if the owner domain name resolves to the owner ID, false otherwise.
func (c *ownerChecker) Check(ctx context.Context) (bool, error) {
	c.logger.Debugf("Resolving owner domain name %s...", c.ownerName)
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	owner, err := c.resolver.LookupTXT(ctx, c.ownerName)
	if err != nil {
		c.logger.Errorf("Could not resolve owner domain name %s: %v", c.ownerName, err)
		return false, fmt.Errorf("could not resolve owner domain name %s: %w", c.ownerName, err)
	}

	var actualOwnerID string
	if len(owner) > 0 {
		actualOwnerID = owner[0]
	}

	switch {
	case actualOwnerID == c.ownerID:
		return true, nil
	default:
		c.logger.Infof("Resolved owner domain name %q to a different owner ID %q than the expected owner ID %q", c.ownerName, actualOwnerID, c.ownerID)
		return false, nil
	}
}
