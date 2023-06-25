package ptr

import (
	"fmt"
	"testing"
)

type DeleteOptions struct {
	GracePeriodSeconds *int64 `json:"gracePeriodSeconds,omitempty" protobuf:"varint,1,opt,name=gracePeriodSeconds"`

	// Deprecated: please use the PropagationPolicy, this field will be deprecated in 1.7.
	// Should the dependent objects be orphaned. If true/false, the "orphan"
	// finalizer will be added to/removed from the object's finalizers list.
	// Either this field or PropagationPolicy may be set, but not both.
	// +optional
	OrphanDependents *bool `json:"orphanDependents,omitempty" protobuf:"varint,3,opt,name=orphanDependents"`
}

func TestPtr(t *testing.T) {
	option := &DeleteOptions{}
	if option == nil {
		fmt.Println("option is nil")
	} else {
		fmt.Println("option is not nil")
	}
	fmt.Println(option == nil)

}

func TestPtr2(t *testing.T) {
	i := new(int64)
	t.Log(*i)
}
