package main

import (
	"math/rand"
	"os"
	"time"

	"k8s.io/kubernetes/cmd/kube-scheduler/app"
	"k8s.io/kubernetes/cmd/kube-scheduler/plugins"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := app.NewSchedulerCommand(
		// 注册插件
		app.WithPlugin(plugins.Name, plugins.New),
	)

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
