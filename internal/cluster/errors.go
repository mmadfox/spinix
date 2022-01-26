package cluster

import "errors"

var (
	ErrAlreadyRunning   = errors.New("cluster: cluster already running")
	ErrNotRunning       = errors.New("cluster: cluster is not running")
	ErrBootstrapTimeout = errors.New("cluster: bootstrap timeout")
	ErrNodeNotFound     = errors.New("cluster: node not found")
	ErrNilOptions       = errors.New("cluster: options cannot be nil")
)
