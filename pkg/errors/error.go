// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package errors

// EtcdError is struct to categorize errors occurred while processing etcd realted operations
type EtcdError struct {
	Message   string
	operation string
}

func (e *EtcdError) Error() string {
	return e.Message
}

// SnapstoreError is struct to categorize errors occurred while processing snapstore realted operations
type SnapstoreError struct {
	Message   string
	operation string
}

func (e *SnapstoreError) Error() string {
	return e.Message
}

// IsErrNotNil checks whether err is nil or not and return boolean.
func IsErrNotNil(err error) bool {
	return err != nil
}
