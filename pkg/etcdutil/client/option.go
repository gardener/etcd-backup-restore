// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
