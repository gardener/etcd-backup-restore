// Copyright (c) 2022 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package client

// Options contains options used by the client.
type Options struct {
	UseServiceEndpoints bool
}

// Option is an interface for changing configuration in client options.
type Option interface {
	ApplyTo(*Options)
}

var _ Option = (*UseServiceEndpoints)(nil)

// UseServiceEndpoints instructs the client to use the service endpoints instead of endpoints.
type UseServiceEndpoints bool

// ApplyTo applies this configuration to the given options.
func (u UseServiceEndpoints) ApplyTo(opt *Options) {
	opt.UseServiceEndpoints = bool(u)
}
