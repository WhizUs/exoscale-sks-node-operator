/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package exoscale

import (
	"context"
	"fmt"
	"sync"
)

// Fake is an in-memory Client for tests. It records every mutation in order so
// tests can assert not just the end state but the sequence — that a pool was
// surged before it was evicted from, and restored on rollback.
type Fake struct {
	mu sync.Mutex

	Cluster   Cluster
	Versions  []string
	Nodepools map[string]*Nodepool

	// Calls records mutating calls in order, e.g. "scale:pool-a:3".
	Calls []string

	// Errors injected per method name ("GetCluster", "ScaleNodepool", ...).
	Errors map[string]error

	// OnScale runs after a successful scale, letting a test simulate the new
	// node appearing (or failing to).
	OnScale func(nodepoolID string, size int64)
	// OnEvict runs after a successful eviction.
	OnEvict func(nodepoolID, instanceID string)
}

var _ Client = (*Fake)(nil)

// NewFake builds a fake with one cluster and the given nodepools.
func NewFake(cluster Cluster, pools ...Nodepool) *Fake {
	f := &Fake{
		Cluster:   cluster,
		Nodepools: map[string]*Nodepool{},
		Errors:    map[string]error{},
	}
	for i := range pools {
		p := pools[i]
		f.Nodepools[p.ID] = &p
	}
	return f
}

func (f *Fake) err(method string) error { return f.Errors[method] }

// GetCluster implements Client.
func (f *Fake) GetCluster(_ context.Context) (*Cluster, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.err("GetCluster"); err != nil {
		return nil, err
	}
	out := f.Cluster
	return &out, nil
}

// ListVersions implements Client.
func (f *Fake) ListVersions(_ context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.err("ListVersions"); err != nil {
		return nil, err
	}
	return append([]string(nil), f.Versions...), nil
}

// GetNodepool implements Client.
func (f *Fake) GetNodepool(_ context.Context, nodepoolID string) (*Nodepool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.err("GetNodepool"); err != nil {
		return nil, err
	}
	np, ok := f.Nodepools[nodepoolID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNodepoolNotFound, nodepoolID)
	}
	out := *np
	return &out, nil
}

// ScaleNodepool implements Client.
func (f *Fake) ScaleNodepool(_ context.Context, nodepoolID string, size int64) error {
	f.mu.Lock()
	if err := f.err("ScaleNodepool"); err != nil {
		f.Calls = append(f.Calls, fmt.Sprintf("scale-failed:%s:%d", nodepoolID, size))
		f.mu.Unlock()
		return err
	}
	np, ok := f.Nodepools[nodepoolID]
	if !ok {
		f.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNodepoolNotFound, nodepoolID)
	}
	np.Size = size
	f.Calls = append(f.Calls, fmt.Sprintf("scale:%s:%d", nodepoolID, size))
	onScale := f.OnScale
	f.mu.Unlock()

	if onScale != nil {
		onScale(nodepoolID, size)
	}
	return nil
}

// EvictNodepoolMember implements Client. Eviction shrinks the pool by one,
// matching the real API.
func (f *Fake) EvictNodepoolMember(_ context.Context, nodepoolID, instanceID string) error {
	f.mu.Lock()
	if err := f.err("EvictNodepoolMember"); err != nil {
		f.Calls = append(f.Calls, fmt.Sprintf("evict-failed:%s:%s", nodepoolID, instanceID))
		f.mu.Unlock()
		return err
	}
	np, ok := f.Nodepools[nodepoolID]
	if !ok {
		f.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNodepoolNotFound, nodepoolID)
	}
	if np.Size > 0 {
		np.Size--
	}
	f.Calls = append(f.Calls, fmt.Sprintf("evict:%s:%s", nodepoolID, instanceID))
	onEvict := f.OnEvict
	f.mu.Unlock()

	if onEvict != nil {
		onEvict(nodepoolID, instanceID)
	}
	return nil
}

// CallLog returns a copy of the recorded mutations.
func (f *Fake) CallLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.Calls...)
}

// PoolSize returns the current size of a nodepool.
func (f *Fake) PoolSize(nodepoolID string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if np, ok := f.Nodepools[nodepoolID]; ok {
		return np.Size
	}
	return -1
}
