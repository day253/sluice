//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package worker

import (
	"syscall"
	"time"
)

func readProcessCPUUsage() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return timevalDuration(usage.Utime) + timevalDuration(usage.Stime), nil
}

func timevalDuration(value syscall.Timeval) time.Duration {
	return time.Duration(value.Sec)*time.Second + time.Duration(value.Usec)*time.Microsecond
}
