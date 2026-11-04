//go:build windows

package main

import (
	"fmt"
	"syscall"
)

var (
	ntdll      = syscall.NewLazyDLL("ntdll.dll")
	kernel32   = syscall.NewLazyDLL("kernel32.dll")
	procSusp   = ntdll.NewProc("NtSuspendProcess")
	procResume = ntdll.NewProc("NtResumeProcess")
	procOpen   = kernel32.NewProc("OpenProcess")
	procClose  = kernel32.NewProc("CloseHandle")
)

const processSuspendResume = 0x0800

func withHandle(pid int, p *syscall.LazyProc) error {
	h, _, err := procOpen.Call(processSuspendResume, 0, uintptr(pid))
	if h == 0 {
		return fmt.Errorf("OpenProcess(%d): %v", pid, err)
	}
	defer func() { _, _, _ = procClose.Call(h) }()
	if r, _, _ := p.Call(h); r != 0 {
		return fmt.Errorf("%s status 0x%x", p.Name, r)
	}
	return nil
}

// pauseProcess freezes every thread of the process (the Windows analogue of SIGSTOP).
func pauseProcess(pid int) error { return withHandle(pid, procSusp) }

// resumeProcess undoes pauseProcess.
func resumeProcess(pid int) error { return withHandle(pid, procResume) }
