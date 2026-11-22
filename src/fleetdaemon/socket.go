package fleetdaemon

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// ListenUnix binds a private local control socket without replacing an
// existing socket/file. It fails closed if the containing directory is
// accessible by other users. Caller must invoke cleanup after server shutdown.
func ListenUnix(socketPath string) (net.Listener, func() error, error) {
	if socketPath == "" {
		return nil, nil, errors.New("socket path required")
	}
	abs, err := filepath.Abs(socketPath)
	if err != nil {
		return nil, nil, err
	}
	if len(abs) > 100 { // Linux AF_UNIX paths must fit sun_path.
		return nil, nil, errors.New("Unix socket path too long")
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, nil, errors.New("socket directory must be private (mode 0700), not a symlink")
	}
	if _, err := os.Lstat(abs); err == nil {
		return nil, nil, fmt.Errorf("socket path already exists: %s; inspect before removing", abs)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	listener, err := net.Listen("unix", abs)
	if err != nil {
		return nil, nil, err
	}
	unix := listener.(*net.UnixListener)
	unix.SetUnlinkOnClose(false)
	if err := os.Chmod(abs, 0600); err != nil {
		listener.Close()
		os.Remove(abs)
		return nil, nil, err
	}
	boundInfo, err := os.Lstat(abs)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	cleanup := func() error {
		closeErr := listener.Close()
		if errors.Is(closeErr, net.ErrClosed) { // http.Server.Shutdown already closed it
			closeErr = nil
		}
		current, err := os.Lstat(abs)
		if errors.Is(err, os.ErrNotExist) {
			return closeErr
		}
		if err != nil {
			return err
		}
		if !os.SameFile(boundInfo, current) {
			return fmt.Errorf("socket changed since bind; refusing to delete %s", abs)
		}
		if err := os.Remove(abs); err != nil {
			return err
		}
		return closeErr
	}
	return listener, cleanup, nil
}
