// Code generated by MockGen. DO NOT EDIT.
// Source: /Users/xxx/projects/spinix/internal/tracker/service.go

// Package mocktracker is a generated GoMock package.
package mocktracker

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	tracker "github.com/mmadfox/spinix/internal/tracker"
	h3 "github.com/uber/h3-go/v3"
)

// MockService is a mock of Service interface.
type MockService struct {
	ctrl     *gomock.Controller
	recorder *MockServiceMockRecorder
}

// MockServiceMockRecorder is the mock recorder for MockService.
type MockServiceMockRecorder struct {
	mock *MockService
}

// NewMockService creates a new mock instance.
func NewMockService(ctrl *gomock.Controller) *MockService {
	mock := &MockService{ctrl: ctrl}
	mock.recorder = &MockServiceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockService) EXPECT() *MockServiceMockRecorder {
	return m.recorder
}

// Add mocks base method.
func (m *MockService) Add(ctx context.Context, object tracker.GeoJSON) ([]h3.H3Index, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Add", ctx, object)
	ret0, _ := ret[0].([]h3.H3Index)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Add indicates an expected call of Add.
func (mr *MockServiceMockRecorder) Add(ctx, object interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockService)(nil).Add), ctx, object)
}

// Detect mocks base method.
func (m *MockService) Detect(ctx context.Context) ([]tracker.Event, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Detect", ctx)
	ret0, _ := ret[0].([]tracker.Event)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Detect indicates an expected call of Detect.
func (mr *MockServiceMockRecorder) Detect(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Detect", reflect.TypeOf((*MockService)(nil).Detect), ctx)
}

// Remove mocks base method.
func (m *MockService) Remove(ctx context.Context, objectID uint64, index []h3.H3Index) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Remove", ctx, objectID, index)
	ret0, _ := ret[0].(error)
	return ret0
}

// Remove indicates an expected call of Remove.
func (mr *MockServiceMockRecorder) Remove(ctx, objectID, index interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Remove", reflect.TypeOf((*MockService)(nil).Remove), ctx, objectID, index)
}
