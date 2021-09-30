// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package common

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// Resolver looks up domain names and returns the corresponding DNS records.
type Resolver interface {
	// LookupTXT returns the DNS TXT records for the given domain name.
	LookupTXT(ctx context.Context, name string) ([]string, error)
}

// NewCachingResolver returns a new Resolver that delegates to the given resolver and caches the results
// for the given ttl.
func NewCachingResolver(resolver Resolver, clock clock.Clock, ttl time.Duration) Resolver {
	return &cachingResolver{
		resolver: resolver,
		clock:    clock,
		ttl:      ttl,
		results:  make(map[string]*result),
	}
}

type cachingResolver struct {
	resolver     Resolver
	clock        clock.Clock
	ttl          time.Duration
	results      map[string]*result
	resultsMutex sync.Mutex
}

type result struct {
	records []string
	err     error
	time    time.Time
}

// LookupTXT returns the DNS TXT records for the given domain name.
func (r *cachingResolver) LookupTXT(ctx context.Context, name string) ([]string, error) {
	r.resultsMutex.Lock()
	defer r.resultsMutex.Unlock()

	// Check if the result is already present in the cache and is more recent than the TTL
	if result, ok := r.results[name]; ok {
		if r.clock.Now().Before(result.time.Add(r.ttl)) {
			return result.records, result.err
		}
		delete(r.results, name)
	}

	// Lookup a new result
	records, err := r.resolver.LookupTXT(ctx, name)
	if ctx.Err() != nil {
		// Avoid caching the result on context errors (cancelled, deadline exceeded)
		return records, err
	}

	// Add the result to the cache and return it
	result := &result{
		records: records,
		err:     err,
		time:    r.clock.Now(),
	}
	r.results[name] = result
	return records, err
}
