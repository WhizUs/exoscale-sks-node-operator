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

// Package exoscale is a narrow view of the Exoscale SKS API: only the calls
// this operator makes, over plain structs, so controllers can be tested without
// network access.
package exoscale

import (
	"context"
	"errors"
	"fmt"

	v3 "github.com/exoscale/egoscale/v3"
	"github.com/exoscale/egoscale/v3/credentials"
)

// Nodepool states this operator cares about. Anything other than running means
// the pool is mid-operation and must be left alone.
const (
	NodepoolStateRunning = "running"
)

// ErrNodepoolNotFound is returned when a nodepool ID is not present on the cluster.
var ErrNodepoolNotFound = errors.New("sks nodepool not found")

// Cluster is an SKS cluster.
type Cluster struct {
	ID      string
	Name    string
	Version string
	State   string
}

// Nodepool is an SKS nodepool.
type Nodepool struct {
	ID      string
	Name    string
	Size    int64
	State   string
	Version string
}

// IsRunning reports whether the pool is idle and safe to mutate.
func (n Nodepool) IsRunning() bool { return n.State == NodepoolStateRunning }

// Client is the Exoscale surface the controllers depend on.
type Client interface {
	// GetCluster returns the SKS cluster this operator manages.
	GetCluster(ctx context.Context) (*Cluster, error)
	// ListVersions returns the Kubernetes versions Exoscale currently offers.
	ListVersions(ctx context.Context) ([]string, error)
	// GetNodepool returns one nodepool of the managed cluster.
	GetNodepool(ctx context.Context, nodepoolID string) (*Nodepool, error)
	// ScaleNodepool sets a nodepool to an absolute size and waits for the
	// operation to settle.
	ScaleNodepool(ctx context.Context, nodepoolID string, size int64) error
	// EvictNodepoolMember removes one instance from a nodepool and waits for the
	// operation to settle. Eviction also shrinks the pool by one.
	EvictNodepoolMember(ctx context.Context, nodepoolID, instanceID string) error
}

// Options configures the API-backed client.
type Options struct {
	// APIKey and APISecret authenticate against Exoscale. Both fall back to
	// EXOSCALE_API_KEY and EXOSCALE_API_SECRET.
	APIKey    string
	APISecret string
	// Zone is the Exoscale zone, e.g. "de-muc-1".
	Zone string
	// ClusterID is the SKS cluster UUID. When empty it is discovered from
	// NodepoolIDHint.
	ClusterID string
	// NodepoolIDHint is a nodepool UUID read off a Node in this cluster, used to
	// identify which SKS cluster the operator is running in.
	NodepoolIDHint string
}

// APIClient talks to the real Exoscale API.
type APIClient struct {
	api       *v3.Client
	clusterID v3.UUID
}

// compile-time check
var _ Client = (*APIClient)(nil)

// New builds a client pinned to one zone and one SKS cluster.
func New(ctx context.Context, opts Options) (*APIClient, error) {
	if opts.Zone == "" {
		return nil, errors.New("exoscale zone is required")
	}

	creds, err := buildCredentials(opts)
	if err != nil {
		return nil, err
	}

	api, err := v3.NewClient(creds)
	if err != nil {
		return nil, fmt.Errorf("build exoscale client: %w", err)
	}

	// The client defaults to the Geneva endpoint, so the zone must be applied
	// explicitly or every call would address the wrong datacentre.
	endpoint, err := api.GetZoneAPIEndpoint(ctx, v3.ZoneName(opts.Zone))
	if err != nil {
		return nil, fmt.Errorf("resolve endpoint for zone %q: %w", opts.Zone, err)
	}
	api = api.WithEndpoint(endpoint)

	clusterID, err := resolveClusterID(ctx, api, opts)
	if err != nil {
		return nil, err
	}

	return &APIClient{api: api, clusterID: clusterID}, nil
}

func buildCredentials(opts Options) (*credentials.Credentials, error) {
	if opts.APIKey != "" && opts.APISecret != "" {
		return credentials.NewStaticCredentials(opts.APIKey, opts.APISecret), nil
	}
	creds := credentials.NewEnvCredentials()
	if _, err := creds.Get(); err != nil {
		return nil, fmt.Errorf("exoscale credentials: %w", err)
	}
	return creds, nil
}

func resolveClusterID(ctx context.Context, api *v3.Client, opts Options) (v3.UUID, error) {
	if opts.ClusterID != "" {
		id, err := v3.ParseUUID(opts.ClusterID)
		if err != nil {
			return v3.UUID(""), fmt.Errorf("parse cluster ID %q: %w", opts.ClusterID, err)
		}
		return id, nil
	}

	if opts.NodepoolIDHint == "" {
		return v3.UUID(""), errors.New(
			"cannot identify the SKS cluster: set spec.exoscale.clusterID, " +
				"or run on a cluster with at least one SKS-managed node")
	}

	resp, err := api.ListSKSClusters(ctx)
	if err != nil {
		return v3.UUID(""), fmt.Errorf("list sks clusters: %w", err)
	}
	for _, c := range resp.SKSClusters {
		for _, np := range c.Nodepools {
			if np.ID.String() == opts.NodepoolIDHint {
				return c.ID, nil
			}
		}
	}
	return v3.UUID(""), fmt.Errorf(
		"no sks cluster in zone %q owns nodepool %s", opts.Zone, opts.NodepoolIDHint)
}

// ClusterID returns the SKS cluster this client is pinned to.
func (c *APIClient) ClusterID() string { return c.clusterID.String() }

// GetCluster implements Client.
func (c *APIClient) GetCluster(ctx context.Context) (*Cluster, error) {
	cluster, err := c.api.GetSKSCluster(ctx, c.clusterID)
	if err != nil {
		return nil, fmt.Errorf("get sks cluster %s: %w", c.clusterID, err)
	}
	return &Cluster{
		ID:      cluster.ID.String(),
		Name:    cluster.Name,
		Version: cluster.Version,
		State:   string(cluster.State),
	}, nil
}

// ListVersions implements Client.
func (c *APIClient) ListVersions(ctx context.Context) ([]string, error) {
	resp, err := c.api.ListSKSClusterVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sks cluster versions: %w", err)
	}
	return resp.SKSClusterVersions, nil
}

// GetNodepool implements Client.
func (c *APIClient) GetNodepool(ctx context.Context, nodepoolID string) (*Nodepool, error) {
	id, err := v3.ParseUUID(nodepoolID)
	if err != nil {
		return nil, fmt.Errorf("parse nodepool ID %q: %w", nodepoolID, err)
	}
	np, err := c.api.GetSKSNodepool(ctx, c.clusterID, id)
	if err != nil {
		return nil, fmt.Errorf("get sks nodepool %s: %w", nodepoolID, err)
	}
	return &Nodepool{
		ID:      np.ID.String(),
		Name:    np.Name,
		Size:    np.Size,
		State:   string(np.State),
		Version: np.Version,
	}, nil
}

// ScaleNodepool implements Client. Size is absolute, not a delta.
func (c *APIClient) ScaleNodepool(ctx context.Context, nodepoolID string, size int64) error {
	id, err := v3.ParseUUID(nodepoolID)
	if err != nil {
		return fmt.Errorf("parse nodepool ID %q: %w", nodepoolID, err)
	}
	op, err := c.api.ScaleSKSNodepool(ctx, c.clusterID, id, v3.ScaleSKSNodepoolRequest{Size: size})
	if err != nil {
		return fmt.Errorf("scale sks nodepool %s to %d: %w", nodepoolID, size, err)
	}
	if err := c.wait(ctx, op); err != nil {
		return fmt.Errorf("scale sks nodepool %s to %d: %w", nodepoolID, size, err)
	}
	return nil
}

// EvictNodepoolMember implements Client.
func (c *APIClient) EvictNodepoolMember(ctx context.Context, nodepoolID, instanceID string) error {
	poolID, err := v3.ParseUUID(nodepoolID)
	if err != nil {
		return fmt.Errorf("parse nodepool ID %q: %w", nodepoolID, err)
	}
	instID, err := v3.ParseUUID(instanceID)
	if err != nil {
		return fmt.Errorf("parse instance ID %q: %w", instanceID, err)
	}

	op, err := c.api.EvictSKSNodepoolMembers(ctx, c.clusterID, poolID,
		v3.EvictSKSNodepoolMembersRequest{Instances: []v3.UUID{instID}})
	if err != nil {
		return fmt.Errorf("evict instance %s from nodepool %s: %w", instanceID, nodepoolID, err)
	}
	if err := c.wait(ctx, op); err != nil {
		return fmt.Errorf("evict instance %s from nodepool %s: %w", instanceID, nodepoolID, err)
	}
	return nil
}

func (c *APIClient) wait(ctx context.Context, op *v3.Operation) error {
	done, err := c.api.Wait(ctx, op, v3.OperationStateSuccess)
	if err != nil {
		return fmt.Errorf("wait for operation: %w", err)
	}
	if done != nil && done.State != v3.OperationStateSuccess {
		return fmt.Errorf("operation finished in state %q: %s", done.State, done.Message)
	}
	return nil
}
