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
	"sync"
	"time"
)

// Default cache lifetimes. The cluster's control-plane version and the list of
// versions Exoscale offers both change on the order of weeks, but the policy
// reconciler reads them on every Node event, and Exoscale rate-limits.
const (
	DefaultClusterTTL  = 2 * time.Minute
	DefaultVersionsTTL = 10 * time.Minute
)

// Cached wraps a Client and memoises the two hot read paths.
//
// Nodepool reads and every mutation deliberately pass straight through: the
// replacement state machine re-reads a pool's size and state immediately before
// scaling and again before evicting, precisely because the cluster-autoscaler
// may have moved them. Serving those from a cache would reintroduce the race the
// re-read exists to close.
type Cached struct {
	inner       Client
	clusterTTL  time.Duration
	versionsTTL time.Duration
	now         func() time.Time

	mu           sync.Mutex
	cluster      *Cluster
	clusterAt    time.Time
	versions     []string
	versionsAt   time.Time
	versionsSeen bool
}

var _ Client = (*Cached)(nil)

// NewCached wraps a client with the default lifetimes.
func NewCached(inner Client) *Cached {
	return &Cached{
		inner:       inner,
		clusterTTL:  DefaultClusterTTL,
		versionsTTL: DefaultVersionsTTL,
		now:         time.Now,
	}
}

// GetCluster implements Client, serving from cache while fresh.
func (c *Cached) GetCluster(ctx context.Context) (*Cluster, error) {
	c.mu.Lock()
	if c.cluster != nil && c.now().Sub(c.clusterAt) < c.clusterTTL {
		cached := *c.cluster
		c.mu.Unlock()
		return &cached, nil
	}
	c.mu.Unlock()

	fresh, err := c.inner.GetCluster(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.cluster, c.clusterAt = fresh, c.now()
	c.mu.Unlock()

	out := *fresh
	return &out, nil
}

// ListVersions implements Client, serving from cache while fresh.
func (c *Cached) ListVersions(ctx context.Context) ([]string, error) {
	c.mu.Lock()
	if c.versionsSeen && c.now().Sub(c.versionsAt) < c.versionsTTL {
		cached := append([]string(nil), c.versions...)
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()

	fresh, err := c.inner.ListVersions(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.versions = append([]string(nil), fresh...)
	c.versionsAt, c.versionsSeen = c.now(), true
	c.mu.Unlock()

	return fresh, nil
}

// GetNodepool implements Client without caching.
func (c *Cached) GetNodepool(ctx context.Context, nodepoolID string) (*Nodepool, error) {
	return c.inner.GetNodepool(ctx, nodepoolID)
}

// ScaleNodepool implements Client and drops the cluster cache, whose nodepool
// sizes the call has just invalidated.
func (c *Cached) ScaleNodepool(ctx context.Context, nodepoolID string, size int64) error {
	err := c.inner.ScaleNodepool(ctx, nodepoolID, size)
	c.InvalidateCluster()
	return err
}

// EvictNodepoolMember implements Client and drops the cluster cache.
func (c *Cached) EvictNodepoolMember(ctx context.Context, nodepoolID, instanceID string) error {
	err := c.inner.EvictNodepoolMember(ctx, nodepoolID, instanceID)
	c.InvalidateCluster()
	return err
}

// InvalidateCluster forces the next GetCluster to hit the API.
func (c *Cached) InvalidateCluster() {
	c.mu.Lock()
	c.cluster = nil
	c.mu.Unlock()
}
