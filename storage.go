package bugfruit

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

// Storage handles reading and writing key/value pairs to/from disk and memory.
type Storage struct {
	name             string   // the name of the database file
	file             *os.File // the database file
	writeCountSync   uint64   // how many write operations since the last fsync
	writeCountVacuum uint64   // how many write operations since the last vacuum

	muFile sync.Mutex // the database file lock
	data   muMap      // the in-memory representation of the data

	idx uint32 // the current index in the file

	closed chan struct{} // this channel is closed when the Storage is closed

	config *Config // configuration for Storage
}

// NewStorage creates a new Storage from a file. If the file does not exist,
// it will be created. If the config is nil, a default VacuumBatch of
// 50,000 and default FsyncBatch of 25,000 will be set.
func NewStorage(filename string, mode os.FileMode, config *Config) (s *Storage, err error) {
	if config == nil {
		config = &Config{
			VacuumBatch: 50000,
			FsyncBatch:  25000,
		}
	}

	s = &Storage{
		name:   filename,
		config: config,
		data:   newMuMap(),
	}

	if s.file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, mode); err != nil {
		return nil, fmt.Errorf("opening database file %s: %w", filename, err)
	}

	for d, err := s.readDatum(); err != io.EOF; d, err = s.readDatum() {
		if err != nil {
			if err2 := s.Close(); err2 != nil {
				return nil, fmt.Errorf("reading datum: while handling error '%v': encountered %w", err, err2)
			}
			return nil, fmt.Errorf("reading datum: %w", err)
		}
		if d != nil {
			s.data.Store(d.key, d)
		}
	}

	return s, nil
}

// Get returns the value for a key and whether the key was found.
func (s *Storage) Get(key string) ([]byte, bool) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, ok
	}
	return val.value, ok
}

// Set sets the key/value pair in-memory and on disk.
// Returns nil on success.
func (s *Storage) Set(key string, value []byte) error {
	if d, exists := s.data.Load(key); exists {
		if err := s.reclaimSpace(d); err != nil {
			return fmt.Errorf("reclaiming datum space: %w", err)
		}
	}
	return s.appendDatum(key, value)
}

// Delete deletes the key/value pair in-memory and on disk.
// Returns nil on success. If the key does not exist in the
// database, error is nil.
func (s *Storage) Delete(key string) error {
	if d, exists := s.data.LoadAndDelete(key); exists {
		if err := s.reclaimSpace(d); err != nil {
			return fmt.Errorf("reclaiming datum space: %w", err)
		}
	}
	return nil
}

// Close and sync the database. Returns nil on success.
func (s *Storage) Close() error {
	s.muFile.Lock()
	defer s.muFile.Unlock()

	// notify vaccuum to stop vaccuuming
	if s.closed != nil {
		close(s.closed)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("closing Storage: %w", err)
	} else if err = s.file.Close(); err != nil {
		return fmt.Errorf("closing Storage: %w", err)
	}
	return nil
}

// Name returns the name of the underlying data file.
func (s *Storage) Name() string {
	return s.name
}

// Snapshot takes a snapshot of the database at the time the function
// is called and writes it to disk at the path indicated by snapname with
// permissions perms. Returns nil on success.
//
// If the file indicated by snapname already exists, it will be deleted
// before being written to.
//
// No writes can occur while Snapshot is taking place.
func (s *Storage) Snapshot(snapname string, perms os.FileMode) error {
	// make sure this is an atomic transaction
	s.data.RLock()
	defer s.data.RUnlock()

	// try to remove the existing file
	if err := os.Remove(snapname); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// make the new storage
	snap, err := NewStorage(snapname, perms, &Config{
		VacuumBatch: 0, // we don't need to vacuum if we don't write deleted data
		FsyncBatch:  0, // we won't need to fsync until the end
	})
	if err != nil {
		return err
	}

	// write the data
	for k, v := range s.data.data {
		if v.Deleted() != byte(1) {
			if err := snap.writeDatumToFile(v); err != nil {
				return fmt.Errorf("setting '%s': %w", k, err)
			}
		}
	}

	return snap.Close()
}

// appendDatum appends a datum to the end of the db file, and
// adds/changes it in the in-memory map.
func (s *Storage) appendDatum(key string, value []byte) (err error) {
	d := newDatum()
	err = d.Set(key, value)
	if err != nil {
		return fmt.Errorf("setting new datum: %w", err)
	}

	s.data.Store(key, d)
	return s.writeDatumToFile(d)
}

// writeDatumToFile persists a datum to disk.
func (s *Storage) writeDatumToFile(d *datum) error {
	s.muFile.Lock()
	defer s.muFile.Unlock()

	// seek to the end of the file
	offset, err := s.file.Seek(0, 2)
	if err != nil {
		return fmt.Errorf("seeking to end of file: %w", err)
	}

	d.idx = uint32(offset)

	if n, err := s.file.Write(d.Bytes()); err != nil {
		return fmt.Errorf("writing to db file: %w", err)
	} else if sz := d.Size(); n != int(sz) {
		return fmt.Errorf("number of bytes written '%d' does not equal size '%d'", n, sz)
	}

	return s.incAndSync()
}

// writeDeletedByte writes the deleted byte of a datum to file.
func (s *Storage) writeDeletedByte(d *datum) error {
	s.muFile.Lock()
	defer s.muFile.Unlock()

	delIdx := int64(d.idx) + metaSize - 1
	if ret, err := s.file.Seek(delIdx, 0); err != nil {
		return fmt.Errorf("seeking to %d: %w", d.idx, err)
	} else if ret != delIdx {
		return fmt.Errorf("seeking to %d: sought to %d instead", d.idx, ret)
	}

	if n, err := s.file.Write([]byte{d.Deleted()}); err != nil {
		return fmt.Errorf("writing to db file: %w", err)
	} else if n != 1 {
		return fmt.Errorf("number of bytes written '%d' does not equal size '1'", n)
	}

	return s.incAndSync()
}

// incAndSync increments the write counter for vacuuming and syncing.
// If the number of writes is greater than or equal to the fsync batch size, the
// file is synced, and the sync counter is reset to 0. If the number of writes is
// greater than or equal to the vacuum batch size, the file is vacuumed, and the
// vacuum counter is reset to 0.
func (s *Storage) incAndSync() error {
	wcs := atomic.AddUint64(&s.writeCountSync, 1)
	wcv := atomic.AddUint64(&s.writeCountVacuum, 1)
	if b := s.config.VacuumBatch; b > 0 && wcv >= b {
		if err := s.vacuum(); err != nil {
			return fmt.Errorf("vacuuming %s: %v", s.name, err)
		}
		atomic.StoreUint64(&s.writeCountVacuum, 0)
	}
	if b := s.config.FsyncBatch; b > 0 && wcs >= b {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("syncing %s: %w", s.name, err)
		}
		atomic.StoreUint64(&s.writeCountSync, 0)
	}
	return nil
}

// reclaimSpace marks a datum as deleted, and marks that
// byte range in the db file as freed.
func (s *Storage) reclaimSpace(d *datum) error {
	d.MarkDeleted()

	if err := s.writeDeletedByte(d); err != nil {
		return fmt.Errorf("updating db file: %w", err)
	}

	return nil
}

// vacuum compacts the database file by removing deleted datums.
func (s *Storage) vacuum() error {
	s.muFile.Lock()
	defer s.muFile.Unlock()

	// seek to beginning of file
	if r, err := s.file.Seek(0, 0); err != nil || r != 0 {
		return fmt.Errorf("tried to seek to index 0, got to %d: %w", r, err)
	}

	// create temp clean db file
	cleaned, err := os.CreateTemp("", "bugfruit-cleanup")
	if err != nil {
		return fmt.Errorf("creating temp db file during vacuum: %w", err)
	}
	cleanedSize := 0
	defer os.Remove(cleaned.Name())

	// read each non-deleted datum from file
	for d, err := s.readDatum(); err != io.EOF; d, err = s.readDatum() {
		if err != nil {
			return fmt.Errorf("reading datum: %w", err)
		}
		if d != nil {
			toWrite := d.Bytes()
			n := len(toWrite)
			cleanedSize += n
			// write our good datum to tmp file
			if written, err := cleaned.Write(d.Bytes()); err != nil || written != n {
				return fmt.Errorf("writing %d bytes to cleanup file, wrote %d: %w", n, written, err)
			}
		}
	}

	// seek back to the beginning of our cleaned tmp file, and regular db file
	if sought, err := cleaned.Seek(0, 0); err != nil || sought != 0 {
		return fmt.Errorf("seeking temporary cleanup file to 0, sought to %d: %w", sought, err)
	} else if r, err := s.file.Seek(0, 0); err != nil || r != 0 {
		return fmt.Errorf("tried to seek to index 0, got to %d: %w", r, err)
	}

	// write the cleaned file to the regular db file
	buf := make([]byte, 1024*5)
	for {
		n, err := cleaned.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading from cleaned db: %w", err)
		} else if n == 0 || err == io.EOF {
			break
		} else if _, err := s.file.Write(buf[:n]); err != nil {
			return err
		}
	}

	// truncate to the appropriate size
	if err := s.file.Truncate(int64(cleanedSize)); err != nil {
		return fmt.Errorf("truncating cleaned db file: %w", err)
	}

	// reset our index to point to the end of the file
	s.idx = uint32(cleanedSize)

	return nil
}

// fileSize returns the size of the underlying data file.
func (s *Storage) fileSize() (uint32, error) {
	s.muFile.Lock()
	defer s.muFile.Unlock()

	fi, err := s.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("statting '%s': %w", s.Name(), err)
	}
	sz := fi.Size()
	if sz > math.MaxUint32 {
		return 0, fmt.Errorf("file size '%d' is too large", sz)
	}
	return uint32(sz), nil
}

// readDatum reads one datum from the file in Storage.
// It is NOT thread safe without external file locking.
func (s *Storage) readDatum() (*datum, error) {
	// read in the meta
	buf := make([]byte, metaSize)
	n, err := s.file.Read(buf)
	if err != nil && ((err != io.EOF) || (err == io.EOF && n != 0)) {
		return nil, fmt.Errorf("reading database file: reading metadata: read %d bytes: %w", n, err)
	} else if err == io.EOF {
		return nil, io.EOF
	}

	// convert to meta
	m := &meta{}
	if err = m.FromBytes(buf); err != nil {
		return nil, fmt.Errorf("reading database file: converting metadata: %w", err)
	}

	totalSize := m.keySize + m.valSize

	// if it's deleted, don't read it in
	if m.deleted == byte(1) {
		// skip to the end of the datum
		if _, err := s.file.Seek(int64(totalSize), 1); err != nil {
			return nil, fmt.Errorf("reading database file: skipping deleted: %w", err)
		}
		// update the current index
		s.idx += totalSize + metaSize
		return nil, nil
	}

	// read total size bytes
	buf = make([]byte, totalSize)
	if n, err = s.file.Read(buf); err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading database file: reading key/val data: %w", err)
	} else if uint32(n) != totalSize {
		return nil, fmt.Errorf("reading database file: reading key/val data: read %d bytes, need %d", n, totalSize)
	}

	// convert to datum
	d := &datum{meta: m, idx: s.idx}
	if err2 := d.KeyValFromBytes(buf); err2 != nil {
		return nil, fmt.Errorf("reading database file: converting key/val data: %w", err2)
	}

	// update the current idx
	s.idx += d.Size()
	return d, nil
}
