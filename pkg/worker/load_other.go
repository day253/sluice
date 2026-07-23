//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package worker

import (
	"fmt"
	"time"
)

func readProcessCPUUsage() (time.Duration, error) {
	return 0, fmt.Errorf("process CPU sampling is unsupported on this platform")
}
