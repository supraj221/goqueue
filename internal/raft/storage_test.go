package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func mkEntries(from, to, term uint64) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Index: i, Term: term, Type: EntryCommand, Data: []byte(fmt.Sprintf("d%d-%d", i, term))})
	}
	return out
}

// storageFactory builds a storage and a way to "crash and reopen" it.
type storageFactory struct {
	name string
	make func(t *testing.T) (st Storage, reopen func() Storage)
}

var factories = []storageFactory{
	{"mem", func(t *testing.T) (Storage, func() Storage) {
		m := NewMemStorage()
		return m, func() Storage { return m }
	}},
	{"file", func(t *testing.T) (Storage, func() Storage) {
		dir := t.TempDir()
		var cur *FileStorage
		open := func() Storage {
			if cur != nil {
				_ = cur.Close()
			}
			fs, err := NewFileStorage(dir)
			if err != nil {
				t.Fatalf("NewFileStorage: %v", err)
			}
			cur = fs
			return fs
		}
		t.Cleanup(func() {
			if cur != nil {
				_ = cur.Close()
			}
		})
		return open(), open
	}},
}

func dumpLog(t *testing.T, st Storage) (first, last uint64, ents []LogEntry) {
	t.Helper()
	var err error
	if first, err = st.FirstIndex(); err != nil {
		t.Fatal(err)
	}
	if last, err = st.LastIndex(); err != nil {
		t.Fatal(err)
	}
	if last >= first {
		if ents, err = st.Entries(first, last+1); err != nil {
			t.Fatal(err)
		}
	}
	return
}

func TestStorageTable(t *testing.T) {
	type step struct {
		name  string
		do    func(t *testing.T, st Storage)
		first uint64
		last  uint64
		terms map[uint64]uint64 // index -> expected term
	}
	steps := []step{
		{"empty", func(t *testing.T, st Storage) {}, 1, 0, nil},
		{"append 1-5 term1", func(t *testing.T, st Storage) {
			if err := st.Append(mkEntries(1, 5, 1)); err != nil {
				t.Fatal(err)
			}
		}, 1, 5, map[uint64]uint64{1: 1, 5: 1}},
		{"append continuing 6-8 term2", func(t *testing.T, st Storage) {
			if err := st.Append(mkEntries(6, 8, 2)); err != nil {
				t.Fatal(err)
			}
		}, 1, 8, map[uint64]uint64{5: 1, 6: 2, 8: 2}},
		{"overlap truncates suffix", func(t *testing.T, st Storage) {
			if err := st.Append(mkEntries(4, 5, 3)); err != nil {
				t.Fatal(err)
			}
		}, 1, 5, map[uint64]uint64{3: 1, 4: 3, 5: 3}},
		{"snapshot compacts prefix", func(t *testing.T, st Storage) {
			if err := st.SaveSnapshot(Snapshot{LastIndex: 3, LastTerm: 1, Data: []byte("snap3")}); err != nil {
				t.Fatal(err)
			}
		}, 4, 5, map[uint64]uint64{3: 1, 4: 3, 5: 3}},
		{"older snapshot ignored", func(t *testing.T, st Storage) {
			if err := st.SaveSnapshot(Snapshot{LastIndex: 2, LastTerm: 1, Data: []byte("old")}); err != nil {
				t.Fatal(err)
			}
		}, 4, 5, map[uint64]uint64{3: 1, 5: 3}},
		{"snapshot with conflicting term drops log", func(t *testing.T, st Storage) {
			if err := st.SaveSnapshot(Snapshot{LastIndex: 5, LastTerm: 9, Data: []byte("snap5")}); err != nil {
				t.Fatal(err)
			}
		}, 6, 5, map[uint64]uint64{5: 9}},
		{"append after snapshot", func(t *testing.T, st Storage) {
			if err := st.Append(mkEntries(6, 7, 9)); err != nil {
				t.Fatal(err)
			}
		}, 6, 7, map[uint64]uint64{5: 9, 6: 9, 7: 9}},
	}
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			st, reopen := f.make(t)
			for _, s := range steps {
				s.do(t, st)
				check := func(label string, st Storage) {
					first, last, _ := dumpLog(t, st)
					if first != s.first || last != s.last {
						t.Fatalf("%s %s: first/last = %d/%d, want %d/%d", s.name, label, first, last, s.first, s.last)
					}
					for idx, term := range s.terms {
						got, err := st.Term(idx)
						if err != nil || got != term {
							t.Fatalf("%s %s: Term(%d) = %d, %v; want %d", s.name, label, idx, got, err, term)
						}
					}
				}
				check("live", st)
				st = reopen()
				check("after reopen", st)
			}
			if snap, _ := st.LoadSnapshot(); string(snap.Data) != "snap5" || snap.LastIndex != 5 {
				t.Fatalf("final snapshot = %+v", snap)
			}
		})
	}
}

func TestStorageErrors(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			st, _ := f.make(t)
			if err := st.Append(mkEntries(3, 4, 1)); err == nil {
				t.Error("append leaving a gap should fail")
			}
			if err := st.Append(mkEntries(1, 3, 1)); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Entries(1, 9); err == nil {
				t.Error("reading past the end should fail")
			}
			if err := st.SaveSnapshot(Snapshot{LastIndex: 2, LastTerm: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Entries(1, 3); err == nil {
				t.Error("reading compacted entries should fail")
			}
			if err := st.Append(mkEntries(2, 3, 1)); err == nil {
				t.Error("append overlapping the snapshot should fail")
			}
		})
	}
}

func TestHardStateRoundTrip(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			st, reopen := f.make(t)
			for i := uint64(1); i <= 5; i++ {
				want := HardState{Term: i, VotedFor: fmt.Sprintf("n%d", i)}
				if err := st.SaveHardState(want); err != nil {
					t.Fatal(err)
				}
				st = reopen()
				got, err := st.LoadHardState()
				if err != nil || got != want {
					t.Fatalf("got %+v, %v; want %+v", got, err, want)
				}
			}
		})
	}
}

// TestFileStorageCorruption damages the WAL in various ways and checks that
// load keeps exactly the intact prefix, truncates the file to it, and stays
// writable afterwards.
func TestFileStorageCorruption(t *testing.T) {
	recLen := func(e LogEntry) int { return recHeaderLen + recMinPayload + len(e.Data) }
	ents := mkEntries(1, 6, 1)
	offsetOf := func(n int) int { // byte offset of the start of record n (0-based)
		off := 0
		for _, e := range ents[:n] {
			off += recLen(e)
		}
		return off
	}
	total := offsetOf(len(ents))

	cases := []struct {
		name     string
		damage   func(data []byte) []byte
		wantLast uint64
	}{
		{"intact", func(d []byte) []byte { return d }, 6},
		{"torn tail: last record cut in half", func(d []byte) []byte { return d[:total-recLen(ents[5])/2] }, 5},
		{"torn tail: only 3 header bytes of next record", func(d []byte) []byte { return append(d[:offsetOf(5)], 1, 2, 3) }, 5},
		{"torn tail: one byte short", func(d []byte) []byte { return d[:total-1] }, 5},
		{"bit flip in payload of record 4", func(d []byte) []byte { d[offsetOf(3)+recHeaderLen+20] ^= 0xff; return d }, 3},
		{"bit flip in CRC of record 2", func(d []byte) []byte { d[offsetOf(1)+5] ^= 0x01; return d }, 1},
		{"bit flip in length of record 5 (huge)", func(d []byte) []byte { d[offsetOf(4)+3] = 0x7f; return d }, 4},
		{"garbage appended", func(d []byte) []byte { return append(d, []byte("not a valid record at all")...) }, 6},
		{"first record corrupt", func(d []byte) []byte { d[recHeaderLen+2] ^= 0xff; return d }, 0},
		{"empty file", func(d []byte) []byte { return nil }, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fs, err := NewFileStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := fs.Append(ents); err != nil {
				t.Fatal(err)
			}
			_ = fs.Close()

			walPath := filepath.Join(dir, walName)
			data, err := os.ReadFile(walPath)
			if err != nil || len(data) != total {
				t.Fatalf("wal length %d (%v), want %d", len(data), err, total)
			}
			if err := os.WriteFile(walPath, tc.damage(data), 0o644); err != nil {
				t.Fatal(err)
			}

			fs, err = NewFileStorage(dir)
			if err != nil {
				t.Fatalf("reopen after damage: %v", err)
			}
			first, last, got := dumpLog(t, fs)
			if first != 1 || last != tc.wantLast {
				t.Fatalf("first/last = %d/%d, want 1/%d", first, last, tc.wantLast)
			}
			if !reflect.DeepEqual(got, ents[:tc.wantLast]) && tc.wantLast > 0 {
				t.Fatalf("surviving entries differ:\n got %v\nwant %v", got, ents[:tc.wantLast])
			}
			// The torn tail must have been cut so new records follow a clean prefix.
			if err := fs.Append(mkEntries(tc.wantLast+1, tc.wantLast+2, 7)); err != nil {
				t.Fatal(err)
			}
			_ = fs.Close()
			fs, err = NewFileStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = fs.Close() }()
			_, last2, _ := dumpLog(t, fs)
			if last2 != tc.wantLast+2 {
				t.Fatalf("after append and reopen last = %d, want %d", last2, tc.wantLast+2)
			}
			if term, _ := fs.Term(last2); term != 7 {
				t.Fatalf("new entry term = %d, want 7", term)
			}
		})
	}
}

func TestFileStorageCorruptHardStateOrSnapshotIsError(t *testing.T) {
	for _, name := range []string{hardStateName, snapshotName} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			fs, err := NewFileStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := fs.SaveHardState(HardState{Term: 3, VotedFor: "n1"}); err != nil {
				t.Fatal(err)
			}
			if err := fs.Append(mkEntries(1, 2, 1)); err != nil {
				t.Fatal(err)
			}
			if err := fs.SaveSnapshot(Snapshot{LastIndex: 2, LastTerm: 1, Data: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			_ = fs.Close()
			p := filepath.Join(dir, name)
			data, _ := os.ReadFile(p)
			data[len(data)-2] ^= 0xff
			if err := os.WriteFile(p, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := NewFileStorage(dir); err == nil {
				t.Fatal("corrupt file should be reported, not silently ignored")
			}
		})
	}
}

// A crash between writing the snapshot file and rewriting the WAL leaves WAL
// records that the snapshot already covers; load must skip them.
func TestFileStorageCrashDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.Append(mkEntries(1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	_ = fs.Close()
	// Write only the snapshot file, as if we crashed before the WAL rewrite.
	if err := writeJSONFile(filepath.Join(dir, snapshotName), Snapshot{LastIndex: 6, LastTerm: 1, Data: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	fs, err = NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fs.Close() }()
	first, last, ents := dumpLog(t, fs)
	if first != 7 || last != 10 || len(ents) != 4 || ents[0].Index != 7 {
		t.Fatalf("first/last/len = %d/%d/%d, want 7/10/4", first, last, len(ents))
	}
}
