package cluster

import (
	"agent-runtime-lab/src/taskevidence"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

type Intent struct {
	Epoch          uint64 `json:"epoch"`
	LocalID        string `json:"local_id"`
	VMRequestSent  bool   `json:"vm_request_sent"`
	JobRequestSent bool   `json:"job_request_sent"`
}
type nodeCheckpoint struct {
	Version     int               `json:"version"`
	Cluster     string            `json:"cluster"`
	Node        string            `json:"node"`
	Incarnation string            `json:"incarnation"`
	Intents     map[string]Intent `json:"intents"`
}
type LocalState struct {
	mu     sync.Mutex
	path   string
	lock   *os.File
	data   nodeCheckpoint
	poison bool
}

var ErrLocalState = errors.New("node intent persistence uncertain; stop and inspect journal")

func OpenLocal(path, clusterID, nodeID string) (*LocalState, error) {
	if !SafeID(clusterID) || !SafeID(nodeID) {
		return nil, ErrConflict
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("node state parent must be private (0700)")
	}
	fd, err := syscall.Open(abs+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "node lock")
	lockInfo, err := lock.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0077 != 0 {
		lock.Close()
		return nil, errors.New("node lock must be a private regular file")
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("node state already owned")
	}
	s := &LocalState{path: abs, lock: lock, data: nodeCheckpoint{Version: 1, Cluster: clusterID, Node: nodeID, Incarnation: Token(), Intents: map[string]Intent{}}}
	fileInfo, statErr := os.Lstat(abs)
	if statErr == nil && (!fileInfo.Mode().IsRegular() || fileInfo.Size() > 1<<20 || fileInfo.Mode().Perm()&0077 != 0) {
		s.Close()
		return nil, errors.New("node checkpoint must be a bounded private regular file")
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		s.Close()
		return nil, statErr
	}
	data, err := os.ReadFile(abs)
	if err == nil {
		info, statErr := os.Lstat(abs)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || decode(data, &s.data) != nil || s.data.Version != 1 || s.data.Cluster != clusterID || s.data.Node != nodeID || !taskevidence.IsHash(s.data.Incarnation) || s.data.Intents == nil || len(s.data.Intents) > MaxJobs {
			s.Close()
			return nil, errors.New("invalid node checkpoint")
		}
		for id, intent := range s.data.Intents {
			if !SafeID(id) || intent.Epoch == 0 || intent.LocalID != localID(clusterID, id, intent.Epoch) {
				s.Close()
				return nil, errors.New("invalid node execution intent")
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		s.Close()
		return nil, err
	}
	if err := s.save(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *LocalState) save() error {
	if s.lock == nil || s.poison {
		return ErrLocalState
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), ".node-*")
	if err != nil {
		s.poison = true
		return err
	}
	defer os.Remove(f.Name())
	fail := func(err error) error { f.Close(); s.poison = true; return err }
	if err := f.Chmod(0600); err != nil {
		return fail(err)
	}
	if _, err := f.Write(encoded(s.data)); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		s.poison = true
		return err
	}
	if err := os.Rename(f.Name(), s.path); err != nil {
		s.poison = true
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		s.poison = true
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		s.poison = true
		return err
	}
	return nil
}

func (s *LocalState) Incarnation() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Incarnation
}
func (s *LocalState) Get(id string) (Intent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison || s.lock == nil {
		return Intent{}, false, ErrLocalState
	}
	v, ok := s.data.Intents[id]
	return v, ok, nil
}

// Prepare happens before the distributed claim, and flags are synced before
// each non-idempotent local RPC. A missing/flagged intent never grants replay.
func (s *LocalState) Prepare(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison || s.lock == nil {
		return ErrLocalState
	}
	old, exists := s.data.Intents[job.Request.ID]
	if exists && old.Epoch == job.Epoch {
		return nil
	}
	if exists && (old.Epoch > job.Epoch || old.VMRequestSent || old.JobRequestSent) {
		return ErrFenced
	}
	if !exists && len(s.data.Intents) >= MaxJobs {
		return ErrFull
	}
	s.data.Intents[job.Request.ID] = Intent{Epoch: job.Epoch, LocalID: job.LocalID}
	return s.save()
}

func (s *LocalState) Mark(id string, epoch uint64, job bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poison || s.lock == nil {
		return false, ErrLocalState
	}
	intent, exists := s.data.Intents[id]
	if !exists || intent.Epoch != epoch {
		return false, ErrFenced
	}
	if job {
		if intent.JobRequestSent {
			return false, nil
		}
		intent.JobRequestSent = true
	} else {
		if intent.VMRequestSent {
			return false, nil
		}
		intent.VMRequestSent = true
	}
	s.data.Intents[id] = intent
	if err := s.save(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *LocalState) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	return err
}
