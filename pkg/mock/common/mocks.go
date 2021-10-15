// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/gardener/etcd-backup-restore/pkg/common (interfaces: Resolver,Checker,Action,Process,ProcessLister)

// Package common is a generated GoMock package.
package common

import (
	context "context"
	reflect "reflect"

	common "github.com/gardener/etcd-backup-restore/pkg/common"
	gomock "github.com/golang/mock/gomock"
)

// MockResolver is a mock of Resolver interface.
type MockResolver struct {
	ctrl     *gomock.Controller
	recorder *MockResolverMockRecorder
}

// MockResolverMockRecorder is the mock recorder for MockResolver.
type MockResolverMockRecorder struct {
	mock *MockResolver
}

// NewMockResolver creates a new mock instance.
func NewMockResolver(ctrl *gomock.Controller) *MockResolver {
	mock := &MockResolver{ctrl: ctrl}
	mock.recorder = &MockResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockResolver) EXPECT() *MockResolverMockRecorder {
	return m.recorder
}

// LookupTXT mocks base method.
func (m *MockResolver) LookupTXT(arg0 context.Context, arg1 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LookupTXT", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LookupTXT indicates an expected call of LookupTXT.
func (mr *MockResolverMockRecorder) LookupTXT(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LookupTXT", reflect.TypeOf((*MockResolver)(nil).LookupTXT), arg0, arg1)
}

// MockChecker is a mock of Checker interface.
type MockChecker struct {
	ctrl     *gomock.Controller
	recorder *MockCheckerMockRecorder
}

// MockCheckerMockRecorder is the mock recorder for MockChecker.
type MockCheckerMockRecorder struct {
	mock *MockChecker
}

// NewMockChecker creates a new mock instance.
func NewMockChecker(ctrl *gomock.Controller) *MockChecker {
	mock := &MockChecker{ctrl: ctrl}
	mock.recorder = &MockCheckerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockChecker) EXPECT() *MockCheckerMockRecorder {
	return m.recorder
}

// Check mocks base method.
func (m *MockChecker) Check(arg0 context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Check", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Check indicates an expected call of Check.
func (mr *MockCheckerMockRecorder) Check(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Check", reflect.TypeOf((*MockChecker)(nil).Check), arg0)
}

// MockAction is a mock of Action interface.
type MockAction struct {
	ctrl     *gomock.Controller
	recorder *MockActionMockRecorder
}

// MockActionMockRecorder is the mock recorder for MockAction.
type MockActionMockRecorder struct {
	mock *MockAction
}

// NewMockAction creates a new mock instance.
func NewMockAction(ctrl *gomock.Controller) *MockAction {
	mock := &MockAction{ctrl: ctrl}
	mock.recorder = &MockActionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAction) EXPECT() *MockActionMockRecorder {
	return m.recorder
}

// Do mocks base method.
func (m *MockAction) Do(arg0 context.Context) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Do", arg0)
}

// Do indicates an expected call of Do.
func (mr *MockActionMockRecorder) Do(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Do", reflect.TypeOf((*MockAction)(nil).Do), arg0)
}

// MockProcess is a mock of Process interface.
type MockProcess struct {
	ctrl     *gomock.Controller
	recorder *MockProcessMockRecorder
}

// MockProcessMockRecorder is the mock recorder for MockProcess.
type MockProcessMockRecorder struct {
	mock *MockProcess
}

// NewMockProcess creates a new mock instance.
func NewMockProcess(ctrl *gomock.Controller) *MockProcess {
	mock := &MockProcess{ctrl: ctrl}
	mock.recorder = &MockProcessMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProcess) EXPECT() *MockProcessMockRecorder {
	return m.recorder
}

// NameWithContext mocks base method.
func (m *MockProcess) NameWithContext(arg0 context.Context) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NameWithContext", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NameWithContext indicates an expected call of NameWithContext.
func (mr *MockProcessMockRecorder) NameWithContext(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NameWithContext", reflect.TypeOf((*MockProcess)(nil).NameWithContext), arg0)
}

// Pid mocks base method.
func (m *MockProcess) Pid() int32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Pid")
	ret0, _ := ret[0].(int32)
	return ret0
}

// Pid indicates an expected call of Pid.
func (mr *MockProcessMockRecorder) Pid() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Pid", reflect.TypeOf((*MockProcess)(nil).Pid))
}

// TerminateWithContext mocks base method.
func (m *MockProcess) TerminateWithContext(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TerminateWithContext", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// TerminateWithContext indicates an expected call of TerminateWithContext.
func (mr *MockProcessMockRecorder) TerminateWithContext(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TerminateWithContext", reflect.TypeOf((*MockProcess)(nil).TerminateWithContext), arg0)
}

// MockProcessLister is a mock of ProcessLister interface.
type MockProcessLister struct {
	ctrl     *gomock.Controller
	recorder *MockProcessListerMockRecorder
}

// MockProcessListerMockRecorder is the mock recorder for MockProcessLister.
type MockProcessListerMockRecorder struct {
	mock *MockProcessLister
}

// NewMockProcessLister creates a new mock instance.
func NewMockProcessLister(ctrl *gomock.Controller) *MockProcessLister {
	mock := &MockProcessLister{ctrl: ctrl}
	mock.recorder = &MockProcessListerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProcessLister) EXPECT() *MockProcessListerMockRecorder {
	return m.recorder
}

// ProcessesWithContext mocks base method.
func (m *MockProcessLister) ProcessesWithContext(arg0 context.Context) ([]common.Process, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProcessesWithContext", arg0)
	ret0, _ := ret[0].([]common.Process)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ProcessesWithContext indicates an expected call of ProcessesWithContext.
func (mr *MockProcessListerMockRecorder) ProcessesWithContext(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProcessesWithContext", reflect.TypeOf((*MockProcessLister)(nil).ProcessesWithContext), arg0)
}
