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

package nodes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

// A representative SKS-managed node.
func sksNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-pool-a-1",
			Labels: map[string]string{
				SKSNodepoolIDLabel:                 "aaaaaaaa-0000-0000-0000-000000000001",
				RegionLabel:                        "de-muc-1",
				"node.kubernetes.io/instance-type": "standard.large",
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "exoscale://bbbbbbbb-0000-0000-0000-000000000001",
		},
		Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.36.2"},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

// A representative Karpenter-managed node.
func karpenterNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "k-node-d-1",
			Labels: map[string]string{
				KarpenterNodePoolLabel:                     "karpenter-pool-d",
				"karpenter.exoscale.com/exoscalenodeclass": "batch",
				"karpenter.sh/registered":                  "true",
				ZoneLabel:                                  "de-muc-1",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: NodeClaimAPIVersion,
				Kind:       NodeClaimKind,
				Name:       "karpenter-pool-d-zr9n7",
			}},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "exoscale://bbbbbbbb-0000-0000-0000-000000000002",
			Taints: []corev1.Taint{
				{Key: "dedicated", Value: "batch", Effect: corev1.TaintEffectNoSchedule},
			},
		},
		Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.36.3"},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestClassify_SKSManaged(t *testing.T) {
	got := Classify(sksNode())

	if got.PoolType != sksv1alpha1.PoolTypeSKSManaged {
		t.Errorf("PoolType = %q, want %q", got.PoolType, sksv1alpha1.PoolTypeSKSManaged)
	}
	if want := "aaaaaaaa-0000-0000-0000-000000000001"; got.SKSNodepoolID != want {
		t.Errorf("SKSNodepoolID = %q, want %q", got.SKSNodepoolID, want)
	}
	if want := "bbbbbbbb-0000-0000-0000-000000000001"; got.InstanceID != want {
		t.Errorf("InstanceID = %q, want %q", got.InstanceID, want)
	}
	if got.KubeletVersion != "v1.36.2" {
		t.Errorf("KubeletVersion = %q, want v1.36.2", got.KubeletVersion)
	}
	if got.Zone != "de-muc-1" {
		t.Errorf("Zone = %q, want de-muc-1", got.Zone)
	}
	if !got.Ready || !got.Managed() {
		t.Errorf("Ready = %v, Managed = %v; want both true", got.Ready, got.Managed())
	}
	if got.NodeClaimName != "" {
		t.Errorf("NodeClaimName = %q, want empty for an SKS node", got.NodeClaimName)
	}
}

func TestClassify_KarpenterManaged(t *testing.T) {
	got := Classify(karpenterNode())

	if got.PoolType != sksv1alpha1.PoolTypeKarpenterManaged {
		t.Errorf("PoolType = %q, want %q", got.PoolType, sksv1alpha1.PoolTypeKarpenterManaged)
	}
	if want := "karpenter-pool-d-zr9n7"; got.NodeClaimName != want {
		t.Errorf("NodeClaimName = %q, want %q", got.NodeClaimName, want)
	}
	if want := "karpenter-pool-d"; got.KarpenterNodePool != want {
		t.Errorf("KarpenterNodePool = %q, want %q", got.KarpenterNodePool, want)
	}
	if want := "bbbbbbbb-0000-0000-0000-000000000002"; got.InstanceID != want {
		t.Errorf("InstanceID = %q, want %q", got.InstanceID, want)
	}
	if got.SKSNodepoolID != "" {
		t.Errorf("SKSNodepoolID = %q, want empty for a Karpenter node", got.SKSNodepoolID)
	}
	if got.Zone != "de-muc-1" {
		t.Errorf("Zone = %q, want de-muc-1", got.Zone)
	}
}

func TestClassify_UnmanagedNodeIsLeftAlone(t *testing.T) {
	got := Classify(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "byo"}})
	if got.Managed() {
		t.Errorf("PoolType = %q, want empty for an unrecognised node", got.PoolType)
	}
}

func TestInstanceIDFromProviderID(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "exoscale://abc-123", want: "abc-123"},
		{in: "aws:///eu-west-1a/i-abc", wantErr: true},
		{in: "exoscale://", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range tests {
		got, err := InstanceIDFromProviderID(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("InstanceIDFromProviderID(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("InstanceIDFromProviderID(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("InstanceIDFromProviderID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestIsReady(t *testing.T) {
	notReady := sksNode()
	notReady.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
	}
	if IsReady(notReady) {
		t.Error("node with Ready=False reported ready")
	}

	noCondition := sksNode()
	noCondition.Status.Conditions = nil
	if IsReady(noCondition) {
		t.Error("node with no Ready condition reported ready")
	}
}

func TestSkipReason(t *testing.T) {
	if got := SkipReason(sksNode()); got != "" {
		t.Errorf("SkipReason(healthy) = %q, want empty", got)
	}

	terminating := sksNode()
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	if SkipReason(terminating) == "" {
		t.Error("terminating node should be skipped")
	}

	annotated := sksNode()
	annotated.Annotations = map[string]string{sksv1alpha1.SkipAnnotation: "true"}
	if SkipReason(annotated) == "" {
		t.Error("node with the skip annotation should be skipped")
	}

	// A node Karpenter is already retiring must not be taken over.
	disrupted := karpenterNode()
	disrupted.Spec.Taints = append(disrupted.Spec.Taints, corev1.Taint{
		Key: KarpenterDisruptedTaint, Effect: corev1.TaintEffectNoSchedule,
	})
	if SkipReason(disrupted) == "" {
		t.Error("node tainted karpenter.sh/disrupted should be skipped")
	}

	doNotDisrupt := karpenterNode()
	doNotDisrupt.Annotations = map[string]string{KarpenterDoNotDisruptAnnotation: "true"}
	if SkipReason(doNotDisrupt) == "" {
		t.Error("node with do-not-disrupt should be skipped")
	}
}
