//go:build !windows

package main

import "syscall"

func pauseProcess(pid int) error  { return syscall.Kill(pid, syscall.SIGSTOP) }
func resumeProcess(pid int) error { return syscall.Kill(pid, syscall.SIGCONT) }
