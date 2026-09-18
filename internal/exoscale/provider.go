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

// Provider hands out a Client for a given zone and cluster.
//
// The client cannot be built at start-up: identifying which SKS cluster the
// operator is running in requires reading a nodepool ID off a Node, which is
// only available once the cache has synced. So it is built on first use and
// rebuilt whenever the configuration it was built from changes.
type Provider interface {
	Get(ctx context.Context, opts Options) (Client, error)
}

// LazyProvider builds an API-backed client on demand and memoises it.
type LazyProvider struct {
	// APIKey and APISecret are the process-wide credentials. Empty falls back to
	// the EXOSCALE_API_KEY and EXOSCALE_API_SECRET environment variables.
	APIKey    string
	APISecret string

	mu     sync.Mutex
	key    string
	client Client
}

var _ Provider = (*LazyProvider)(nil)

// Get returns a cached client, rebuilding it if the options changed.
func (p *LazyProvider) Get(ctx context.Context, opts Options) (Client, error) {
	opts.APIKey, opts.APISecret = p.APIKey, p.APISecret
	key := fmt.Sprintf("%s|%s|%s", opts.Zone, opts.ClusterID, opts.NodepoolIDHint)

	p.mu.Lock()
	if p.client != nil && p.key == key {
		c := p.client
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()

	api, err := New(ctx, opts)
	if err != nil {
		return nil, err
	}
	client := NewCached(api)

	p.mu.Lock()
	p.client, p.key = client, key
	p.mu.Unlock()

	return client, nil
}

// StaticProvider always returns the same client. Used by tests.
type StaticProvider struct {
	Client Client
	// LastOptions records what the caller resolved before asking, so tests can
	// assert zone and cluster discovery rather than only the end result.
	LastOptions Options
}

var _ Provider = (*StaticProvider)(nil)

// Get implements Provider.
func (p *StaticProvider) Get(_ context.Context, opts Options) (Client, error) {
	p.LastOptions = opts
	if p.Client == nil {
		return nil, fmt.Errorf("no exoscale client configured")
	}
	return p.Client, nil
}
