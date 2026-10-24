package processinfo

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseNproc(t *testing.T) {
	for _, test := range []struct {
		input, soft, hard string
		valid             bool
	}{
		{"Max processes             64                   256                  processes\n", "64", "256", true},
		{"Max processes unlimited unlimited processes", "unlimited", "unlimited", true},
		{"Max processes bad 64 processes", "", "", false},
		{"Max open files 256 256 files", "", "", false},
	} {
		limit, err := ParseNproc(test.input)
		if (err == nil) != test.valid {
			t.Fatalf("input=%q limit=%+v err=%v", test.input, limit, err)
		}
		if test.valid && (limit.Soft != test.soft || limit.Hard != test.hard) {
			t.Fatalf("%+v", limit)
		}
	}
}

func TestProcTaskEntriesAndIncompleteAccounting(t *testing.T) {
	root := t.TempDir()
	for _, task := range []string{"11", "12", "13"} {
		if err := os.MkdirAll(filepath.Join(root, "11", "task", task), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string]string{"status": "Name:\tprobe\nState:\tS (sleeping)\nUid:\t65532 65532 65532 65532\n", "limits": "Max processes 64 256 processes\n"} {
		if err := os.WriteFile(filepath.Join(root, "11", name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s := ReadProcesses(root, "fixture proc", []int{11})
	if s.Threads == nil || *s.Threads != 3 || len(s.Processes) != 1 || *s.Processes[0].RealUID != 65532 || s.Processes[0].Nproc.Soft != "64" {
		t.Fatalf("%+v", s)
	}
	s = ReadProcesses(root, "fixture proc", []int{11, 99})
	if s.Threads != nil || len(s.Errors) == 0 {
		t.Fatalf("missing process must not become a fabricated zero: %+v", s)
	}
}
