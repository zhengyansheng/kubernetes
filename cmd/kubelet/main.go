package main

import (
	"fmt"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"
	"k8s.io/kubernetes/pkg/kubelet/cri/remote"
)

func main() {
	//kubeletFlags := options.NewKubeletFlags()
	//fmt.Printf("kubeletFlags: %+v\n", kubeletFlags)

	const connectionTimeout = 2 * time.Minute
	runtimeEndpoint := "unix:///run/containerd/containerd.sock"
	r, err := remote.NewRemoteRuntimeService(runtimeEndpoint, connectionTimeout, oteltrace.NewNoopTracerProvider())
	if err != nil {
		panic(err)
	}
	containers, err := r.ListContainers(nil, nil)
	if err != nil {
		return
	}
	for _, container := range containers {
		fmt.Println(container.Id)
	}
}
