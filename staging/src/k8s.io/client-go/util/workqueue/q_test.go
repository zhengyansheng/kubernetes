package workqueue

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/utils/clock"
)

func TestNewTickerClock(t *testing.T) {
	heartbeat := clock.RealClock{}
	ticker := heartbeat.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C():
			return
		default:
			time.Sleep(time.Second * 1)
			fmt.Println("sleep")
		}
	}
}
