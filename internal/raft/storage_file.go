package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage is a durable Storage backed by three files in one directory:
//
//	wal       append-only log of length-prefixed, CRC32-checked entry records
//	hardstate term and vote, replaced atomically (temp file, fsync, rename)
//	snapshot  the latest snapshot, replaced atomically
//
// WAL record layout (little endian):
//
//	[4 bytes payload length][4 bytes CRC32-IEEE of payload][payload]
//	payload = [8 bytes index][8 bytes term][1 byte type][data...]
//
// There is no special truncation record. A record whose index is at or below
// the current last index replaces that entry and everything after it, which is
// exactly what Append with an overlapping index means. On load a torn or
// corrupt tail is cut off.
//
// The whole log is also kept in memory, so reads never touch the disk.
type FileStorage struct {
	mu  sync.Mutex
	dir string
	wal *os.File
	mem MemStorage
}

const (
	walName       = "wal"
	hardStateName = "hardstate"
	snapshotName  = "snapshot"

	recHeaderLen  = 8
	recMinPayload = 17
	recMaxPayload = 1 << 30
)

// NewFileStorage opens (creating if needed) a FileStorage in dir, recovering
// from any torn WAL tail.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStorage{dir: dir}
	// Leftover temp files are from a crash mid-write and are safe to drop.
	for _, name := range []string{hardStateName, snapshotName, walName} {
		_ = os.Remove(filepath.Join(dir, name+".tmp"))
	}
	var err error
	if fs.mem.hs, err = loadJSONFile[HardState](filepath.Join(dir, hardStateName)); err != nil {
		return nil, fmt.Errorf("raft: load hardstate: %w", err)
	}
	if fs.mem.snap, err = loadJSONFile[Snapshot](filepath.Join(dir, snapshotName)); err != nil {
		return nil, fmt.Errorf("raft: load snapshot: %w", err)
	}
	if err := fs.loadWAL(); err != nil {
		return nil, fmt.Errorf("raft: load wal: %w", err)
	}
	return fs, nil
}

func encodeRecord(e LogEntry) []byte {
	plen := recMinPayload + len(e.Data)
	buf := make([]byte, recHeaderLen+plen)
	p := buf[recHeaderLen:]
	binary.LittleEndian.PutUint64(p[0:], e.Index)
	binary.LittleEndian.PutUint64(p[8:], e.Term)
	p[16] = byte(e.Type)
	copy(p[17:], e.Data)
	binary.LittleEndian.PutUint32(buf[0:], uint32(plen))
	binary.LittleEndian.PutUint32(buf[4:], crc32.ChecksumIEEE(p))
	return buf
}

func (fs *FileStorage) loadWAL() error {
	path := filepath.Join(fs.dir, walName)
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	snapIdx := fs.mem.snap.LastIndex
	var ents []LogEntry
	off, good := 0, 0
scan:
	for off+recHeaderLen <= len(data) {
		plen := int(binary.LittleEndian.Uint32(data[off:]))
		crc := binary.LittleEndian.Uint32(data[off+4:])
		if plen < recMinPayload || plen > recMaxPayload || off+recHeaderLen+plen > len(data) {
			break
		}
		p := data[off+recHeaderLen : off+recHeaderLen+plen]
		if crc32.ChecksumIEEE(p) != crc {
			break
		}
		e := LogEntry{
			Index: binary.LittleEndian.Uint64(p[0:]),
			Term:  binary.LittleEndian.Uint64(p[8:]),
			Type:  EntryType(p[16]),
		}
		if plen > recMinPayload {
			e.Data = append([]byte(nil), p[17:]...)
		}
		if e.Index > snapIdx {
			next := snapIdx + uint64(len(ents)) + 1
			switch {
			case e.Index > next:
				break scan // gap: treat as corruption
			case e.Index < next:
				ents = ents[:e.Index-snapIdx-1]
			}
			ents = append(ents, e)
		}
		off += recHeaderLen + plen
		good = off
	}
	fs.mem.ents = ents

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if good < len(data) {
		if err := f.Truncate(int64(good)); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if _, err := f.Seek(int64(good), 0); err != nil {
		_ = f.Close()
		return err
	}
	fs.wal = f
	return nil
}

func (fs *FileStorage) LoadHardState() (HardState, error) { return fs.mem.LoadHardState() }
func (fs *FileStorage) FirstIndex() (uint64, error)       { return fs.mem.FirstIndex() }
func (fs *FileStorage) LastIndex() (uint64, error)        { return fs.mem.LastIndex() }
func (fs *FileStorage) LoadSnapshot() (Snapshot, error)   { return fs.mem.LoadSnapshot() }
func (fs *FileStorage) Term(i uint64) (uint64, error)     { return fs.mem.Term(i) }
func (fs *FileStorage) Entries(lo, hi uint64) ([]LogEntry, error) {
	return fs.mem.Entries(lo, hi)
}

func (fs *FileStorage) SaveHardState(hs HardState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := writeJSONFile(filepath.Join(fs.dir, hardStateName), hs); err != nil {
		return err
	}
	return fs.mem.SaveHardState(hs)
}

// Append writes the records and fsyncs the WAL before returning.
func (fs *FileStorage) Append(entries []LogEntry) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.mem.mu.Lock()
	err := fs.mem.checkAppendLocked(entries)
	fs.mem.mu.Unlock()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if fs.wal == nil {
		return errors.New("raft: storage closed")
	}
	var buf []byte
	for _, e := range entries {
		buf = append(buf, encodeRecord(e)...)
	}
	if _, err := fs.wal.Write(buf); err != nil {
		return err
	}
	if err := fs.wal.Sync(); err != nil {
		return err
	}
	return fs.mem.Append(entries)
}

// SaveSnapshot writes the snapshot atomically, then rewrites the WAL without
// the compacted prefix. A crash between the two steps is safe: load skips WAL
// records at or below the snapshot index.
func (fs *FileStorage) SaveSnapshot(snap Snapshot) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cur, _ := fs.mem.LoadSnapshot()
	if snap.LastIndex <= cur.LastIndex {
		return nil
	}
	if err := writeJSONFile(filepath.Join(fs.dir, snapshotName), snap); err != nil {
		return err
	}
	fs.mem.mu.Lock()
	fs.mem.applySnapshotLocked(snap)
	rest := append([]LogEntry(nil), fs.mem.ents...)
	fs.mem.mu.Unlock()
	return fs.rewriteWAL(rest)
}

func (fs *FileStorage) rewriteWAL(ents []LogEntry) error {
	path := filepath.Join(fs.dir, walName)
	tmp := path + ".tmp"
	var buf []byte
	for _, e := range ents {
		buf = append(buf, encodeRecord(e)...)
	}
	if err := writeFileSync(tmp, buf); err != nil {
		return err
	}
	// Windows cannot rename over an open file, so release the handle first.
	if fs.wal != nil {
		if err := fs.wal.Close(); err != nil {
			return err
		}
		fs.wal = nil
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(fs.dir)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return err
	}
	fs.wal = f
	return nil
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.wal == nil {
		return nil
	}
	err := fs.wal.Close()
	fs.wal = nil
	return err
}

var _ Storage = (*FileStorage)(nil)

// writeFileSync writes data to path and fsyncs it.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// writeJSONFile atomically replaces path with [crc32][json(v)]: write a temp
// file, fsync it, rename it over the old one. On Windows os.Rename replaces an
// existing destination.
func writeJSONFile(path string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	buf := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(buf, crc32.ChecksumIEEE(body))
	copy(buf[4:], body)
	tmp := path + ".tmp"
	if err := writeFileSync(tmp, buf); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// loadJSONFile reads a file written by writeJSONFile. A missing file yields
// the zero value; a corrupt one is an error.
func loadJSONFile[T any](path string) (T, error) {
	var v T
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	if len(data) < 4 || crc32.ChecksumIEEE(data[4:]) != binary.LittleEndian.Uint32(data) {
		return v, errors.New("checksum mismatch")
	}
	if err := json.Unmarshal(data[4:], &v); err != nil {
		return v, err
	}
	return v, nil
}

// syncDir flushes directory metadata so a rename survives power loss. It is
// best effort: Windows cannot fsync a directory.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
