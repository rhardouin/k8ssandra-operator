package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	"github.com/k8ssandra/cass-operator/pkg/httphelper"
	"github.com/k8ssandra/k8ssandra-operator/pkg/cassandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/mocks"
	"github.com/stretchr/testify/mock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	StargateAuthKeyspace = "data_endpoint_auth"
	StargateAuthTable    = "token"
)

type ManagementApiFactoryAdapter func(
	ctx context.Context,
	datacenter *cassdcapi.CassandraDatacenter,
	client client.Client,
	logger logr.Logger) (cassandra.ManagementApiFacade, error)

var defaultAdapter ManagementApiFactoryAdapter = func(
	ctx context.Context,
	datacenter *cassdcapi.CassandraDatacenter,
	client client.Client,
	logger logr.Logger) (cassandra.ManagementApiFacade, error) {

	m := new(mocks.ManagementApiFacade)
	m.On(EnsureKeyspaceReplication, mock.Anything, mock.Anything).Return(nil)
	m.On(ListTables, StargateAuthKeyspace).Return([]string{"token"}, nil)
	m.On(CreateTable, mock.MatchedBy(func(def *httphelper.TableDefinition) bool {
		return def.KeyspaceName == StargateAuthKeyspace && def.TableName == StargateAuthTable
	})).Return(nil)
	m.On(ListKeyspaces, "").Return([]string{}, nil)
	m.On(GetSchemaVersions).Return(map[string][]string{"fake": {"test"}}, nil)
	return m, nil
}

type FakeManagementApiFactory struct {
	mutex sync.RWMutex
	t     *testing.T

	adapter ManagementApiFactoryAdapter
}

func (f *FakeManagementApiFactory) SetT(t *testing.T) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.t = t
}

func (f *FakeManagementApiFactory) UseDefaultAdapter() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.adapter = defaultAdapter
}

func (f *FakeManagementApiFactory) SetAdapter(a ManagementApiFactoryAdapter) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.adapter = a
}

func (f *FakeManagementApiFactory) NewManagementApiFacade(
	ctx context.Context,
	dc *cassdcapi.CassandraDatacenter,
	client client.Client,
	logger logr.Logger) (cassandra.ManagementApiFacade, error) {
	f.mutex.RLock()
	t := f.t
	adapter := f.adapter
	f.mutex.RUnlock()

	if t == nil {
		return nil, fmt.Errorf("testing.T instance not set")
	}

	if adapter == nil {
		return nil, fmt.Errorf("adapter not set")
	}

	var mgmtApi cassandra.ManagementApiFacade
	var err error

	mgmtApi, err = adapter(ctx, dc, client, logger)
	if err != nil {
		return nil, err
	}

	if testable, ok := mgmtApi.(Testable); ok {
		testable.Test(t)
	}

	return mgmtApi, nil
}

type ManagementApiMethod string

const (
	EnsureKeyspaceReplication = "EnsureKeyspaceReplication"
	GetKeyspaceReplication    = "GetKeyspaceReplication"
	CreateKeyspaceIfNotExists = "CreateKeyspaceIfNotExists"
	AlterKeyspace             = "AlterKeyspace"
	ListKeyspaces             = "ListKeyspaces"
	CreateTable               = "CreateTable"
	ListTables                = "ListTables"
	GetSchemaVersions         = "GetSchemaVersions"
)

type FakeManagementApiFacade struct {
	*mocks.ManagementApiFacade

	callsMutex    sync.RWMutex
	recordedCalls []mock.Call
}

type Testable interface {
	Test(t mock.TestingT)
}

func NewFakeManagementApiFacade() *FakeManagementApiFacade {
	m := new(mocks.ManagementApiFacade)
	return &FakeManagementApiFacade{ManagementApiFacade: m}
}

// EnsureKeyspaceReplication records completed calls in a race-safe log used by
// controller tests that assert call ordering while reconciliation is active.
func (f *FakeManagementApiFacade) EnsureKeyspaceReplication(keyspaceName string, replication map[string]int) error {
	err := f.ManagementApiFacade.EnsureKeyspaceReplication(keyspaceName, replication)
	f.callsMutex.Lock()
	defer f.callsMutex.Unlock()
	f.recordedCalls = append(f.recordedCalls, mock.Call{
		Method:    string(EnsureKeyspaceReplication),
		Arguments: mock.Arguments{keyspaceName, replication},
	})
	return err
}

func (f *FakeManagementApiFacade) GetLastCall(method ManagementApiMethod, args ...interface{}) int {
	f.callsMutex.RLock()
	defer f.callsMutex.RUnlock()
	idx := -1

	calls := make([]mock.Call, 0)
	for _, call := range f.recordedCalls {
		if call.Method == string(method) {
			calls = append(calls, call)
		}
	}

	for i, call := range calls {
		if _, count := call.Arguments.Diff(args); count == 0 {
			idx = i
		}
	}

	return idx
}

func (f *FakeManagementApiFacade) GetFirstCall(method ManagementApiMethod, args ...interface{}) int {
	f.callsMutex.RLock()
	defer f.callsMutex.RUnlock()
	calls := make([]mock.Call, 0)
	for _, call := range f.recordedCalls {
		if call.Method == string(method) {
			calls = append(calls, call)
		}
	}

	for i, call := range calls {
		if _, count := call.Arguments.Diff(args); count == 0 {
			return i
		}
	}

	return -1
}
