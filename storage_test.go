package bugfruit

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/reesporte/bugfruit/test"
)

// TestReadDatum ensures that reading datum that
// have been written to file does not result in unexpected errors.
func TestReadDatum(t *testing.T) {
	// initialize a Storage
	s := &Storage{}
	fname := filepath.Join(t.TempDir(), "test-read-datum")

	// create a test db file
	var err error
	s.file, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0644)
	test.AssertNil(t, err)
	defer s.file.Close()

	kvs := []struct {
		k     string
		v     []byte
		toDel bool
	}{
		{
			k: "galadriel",
			v: []byte("I amar prestar aen"),
		},
		{
			k: "frodo",
			v: []byte("He deserves death"),
		},
		{
			k: "boromir",
			v: []byte("You are no Elf."),
		},
	}
	datums := []*datum{}
	// add some data to the test db file
	idx := uint32(0)
	for _, kv := range kvs {
		d := newDatum()
		err = d.Set(kv.k, kv.v)
		test.AssertNil(t, err)

		if kv.toDel {
			d.MarkDeleted()
		}

		d.idx = idx
		b := d.Bytes()

		n, err := s.file.Write(b)
		test.AssertNil(t, err)
		test.AssertEqual(t, n, len(b))

		idx += uint32(n)
		datums = append(datums, d)
	}

	// seek the file back to the start
	off, err := s.file.Seek(0, 0)
	test.AssertNil(t, err)
	test.AssertEqual(t, int(0), int(off))

	curIdx := uint32(0)
	test.AssertEqual(t, curIdx, s.idx)

	for i, kv := range kvs {
		// read the datums
		d := datums[i]
		d2, err := s.readDatum()
		test.AssertNil(t, err)
		if !kv.toDel {
			test.AssertEqual(t, d, d2)
		} else {
			test.AssertEqual(t, nil, d2)
		}
		curIdx += d.Size()
		test.AssertEqual(t, curIdx, s.idx)
	}

	// reading past end of file results in EOF
	datum4, err := s.readDatum()
	test.AssertEqual(t, io.EOF, err)
	test.AssertEqual(t, (*datum)(nil), datum4)
	test.AssertEqual(t, curIdx, s.idx)
}

// TestReadDatumCorruptVal ensures that reading corrupt datum that
// have been written to file does not result in unexpected errors.
func TestReadDatumCorruptVal(t *testing.T) {
	// initialize a Storage
	fname := filepath.Join(t.TempDir(), "test-read-datum-corrupt-val")
	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	// make corrupt data
	galadriel := newDatum()
	err = galadriel.Set("galadriel", []byte("I amar prestar aen"))
	test.AssertNil(t, err)
	b := galadriel.Bytes()
	b = b[:len(b)-4]

	// write the corrupt data to file
	n, err := s.file.Write(b)
	test.AssertNil(t, err)
	test.AssertEqual(t, len(b), n)

	// seek the file back to the start
	off, err := s.file.Seek(0, 0)
	test.AssertNil(t, err)
	test.AssertEqual(t, int(0), int(off))

	// read the first datum
	galadriel2, err := s.readDatum()
	exp := fmt.Errorf("reading database file: reading key/val data: read %d bytes, need %d", len(b)-metaSize, len(b)-metaSize+4)
	test.AssertEqual(t, exp.Error(), err.Error())
	test.AssertEqual(t, (*datum)(nil), galadriel2)
}

// TestNewStorage ensures that a new Storage can be created safely. It also ensures
// that Storage will err if the file is not a regular file.
func TestNewStorage(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "gimli_and_galadriel")
	// normal path
	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)
	test.AssertNotEqual(t, (*Storage)(nil), s)
	test.AssertEqual(t, fname, s.Name())
	s.Close()

	// bad path: writing on a directory
	tdir := t.TempDir()
	s, err = NewStorage(tdir, 0644, nil)
	test.AssertEqual(t, (*Storage)(nil), s)

	exp := fmt.Errorf("opening database file %s: open %s: is a directory", tdir, tdir)
	test.AssertEqual(t, exp.Error(), err.Error())
}

// TestIntegratedStorageWithData ensures that reading datum that have been written
// to file does not result in unexpected errors.
func TestIntegratedStorageWithData(t *testing.T) {
	fname := createTestDBFile(t)

	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	kvs := []struct {
		k     string
		v     []byte
		notOk bool
	}{
		{
			k: "galadriel",
			v: []byte("I amar prestar aen"),
		},
		{
			k: "frodo",
			v: []byte("He deserves death"),
		},
		{
			k: "gimli",
			v: []byte("Don't tell the elf!"),
		},
		{
			k:     "legolas",
			v:     []byte(nil),
			notOk: true,
		},
	}

	for _, kv := range kvs {
		got, ok := s.Get(kv.k)
		test.AssertEqual(t, kv.v, got)
		test.AssertEqual(t, kv.notOk, !ok)
	}
}

// This isn't a general-purpose helper function, I just use it to
// easily make a test file for Storage stuff.
func createTestDBFile(t *testing.T) string {
	t.Helper()
	fname := filepath.Join(t.TempDir(), "NewStorageWithData")

	var err error
	file, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0644)
	test.AssertNil(t, err)

	kvs := []struct {
		k string
		v []byte
	}{
		{
			k: "galadriel",
			v: []byte("I amar prestar aen"),
		},
		{
			k: "frodo",
			v: []byte("He deserves death"),
		},
		{
			k: "gimli",
			v: []byte("Don't tell the elf!"),
		},
	}

	for _, kv := range kvs {
		d := newDatum()
		err = d.Set(kv.k, kv.v)
		test.AssertNil(t, err)
		b := d.Bytes()
		n, err := file.Write(b)
		test.AssertNil(t, err)
		test.AssertEqual(t, len(b), n)
	}

	file.Close()

	return fname
}

// TestWriteDatumToFile ensures that calling s.writeDatumToFile writes
// the datum to file without error.
func TestWriteDatumToFile(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "testing-testing-one-two-three")

	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	// make some test data
	d := newDatum()
	err = d.Set("aragorn", []byte("He's trying to bring down the mountain!"))
	test.AssertNil(t, err)

	bytes := d.Bytes()

	err = s.writeDatumToFile(d)
	test.AssertNil(t, err)

	s.Close()

	b, err := os.ReadFile(fname)
	test.AssertNil(t, err)

	test.AssertEqual(t, b, bytes)
}

// TestFileSize ensures that calling s.fileSize() returns the correct file size.
func TestFileSize(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "testing-testing-one-two-three")

	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	// make some test data
	key, value := "aragorn", []byte("He's trying to bring down the mountain!")
	d := newDatum()
	err = d.Set(key, value)
	test.AssertNil(t, err)
	err = s.Set(key, value)
	test.AssertNil(t, err)

	// normal path
	got, err := s.fileSize()
	test.AssertNil(t, err)
	test.AssertEqual(t, d.Size(), got)

	// weird path
	err = s.Close()
	test.AssertNil(t, err)

	got, err = s.fileSize()
	exp := fmt.Errorf("statting '%s': stat %s: use of closed file", fname, fname)
	test.AssertEqual(t, exp.Error(), err.Error())
	test.AssertEqual(t, uint32(0), got)
}

// TestReclaimSpace ensures that calling s.reclaimSpace on
// a datum marks that datum as deleted.
func TestReclaimSpace(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "testing-testing-one-two-three")

	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	k, v := "gandalf", []byte("Fool of a Took!")
	err = s.Set(k, v)
	test.AssertNil(t, err)

	d, ok := s.data.Load(k)
	test.AssertEqual(t, true, ok)

	err = s.reclaimSpace(d)
	test.AssertNil(t, err)

	test.AssertEqual(t, byte(1), d.Deleted())

	// ensure the datum is marked as deleted
	_, err = s.file.Seek(int64(d.idx)+metaSize-1, 0)
	test.AssertNil(t, err)

	b := make([]byte, 1)

	_, err = s.file.Read(b)
	test.AssertNil(t, err)

	test.AssertEqual(t, byte(1), b[0])
}

// TestAppendDatum ensures that appendDatum appends a datum to the end of the
// db file and adds it to the in-memory map.
func TestAppendDatum(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "testing-testing-one-two-three")

	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	k, v := "legolas", []byte("That is no orc horn.")
	err = s.appendDatum(k, v)
	test.AssertNil(t, err)

	legolas := newDatum()
	err = legolas.Set(k, v)
	test.AssertNil(t, err)

	got, ok := s.data.Load("legolas")
	test.AssertEqual(t, true, ok)
	test.AssertEqual(t, legolas, got)

	_, err = s.file.Seek(0, 0)
	test.AssertNil(t, err)

	buf := make([]byte, legolas.Size())
	_, err = s.file.Read(buf)
	test.AssertNil(t, err)

	test.AssertEqual(t, legolas.Bytes(), buf)
}

// TestDelete ensures that calling Delete on Storage
// deletes the key/value pair.
func TestDelete(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "testing-testing-one-two-three")

	s, err := NewStorage(fname, 0644, nil)
	test.AssertNil(t, err)

	k, v := "aragorn", []byte("You will suffer me.")

	// deleting something that doesn't exist returns nil
	err = s.Delete(k)
	test.AssertNil(t, err)

	// set the thing
	err = s.Set(k, v)
	test.AssertNil(t, err)

	// make sure it's there:
	aragorn, ok := s.Get(k)
	test.AssertEqual(t, true, ok)
	test.AssertEqual(t, v, aragorn)

	// delete it
	err = s.Delete(k)
	test.AssertNil(t, err)

	// get it again, should be nil
	aragorn, ok = s.Get(k)
	test.AssertEqual(t, false, ok)
	test.AssertEqual(t, []byte(nil), aragorn)
}

// TestVacuum ensures that s.vacuum compacts
// the database file by removing deleted data.
//
// It also tests to a small extent that one can safely
// and concurrently run Set/Delete operations on Storage.
func TestVacuum(t *testing.T) {
	fname := filepath.Join(t.TempDir(), "testing-testing-one-two-three")
	// set ticks to 0 so that we can call vacuum manually
	s, err := NewStorage(fname, 0644, &Config{VacuumBatch: 0})
	test.AssertNil(t, err)

	expected := new(bytes.Buffer)

	type kv struct {
		key string
		val []byte
	}
	pairs := []kv{
		{
			key: "elrond",
			val: []byte("Let him not vow to walk in the dark, who has not seen the nightfall."),
		},
		{
			key: "tom",
			val: []byte("Ring a ding dillo!"),
		},
		{
			key: "sauron",
			val: []byte("You cannot hide, I see you!"),
		},
		{
			key: "gandalf",
			val: []byte("Fly, you fools!"),
		},
		{
			key: "faramir",
			val: []byte("War will make corpses of us all."),
		},
	}

	toDelete := map[string]struct{}{
		"sauron":  struct{}{},
		"faramir": struct{}{},
	}

	var wg sync.WaitGroup
	order := make(chan kv, len(pairs))

	for _, pair := range pairs {
		p := pair
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Set(p.key, p.val)
			test.AssertNil(t, err)
			order <- p
		}()

	}

	wg.Wait()

	// write the bytes to file in the order they come in
	for i := 0; i < len(pairs); i++ {
		p := <-order
		if _, ok := toDelete[p.key]; !ok {
			d := newDatum()
			err := d.Set(p.key, p.val)
			test.AssertNil(t, err)
			expected.Write(d.Bytes())
		}
	}

	for deleteMe := range toDelete {
		wg.Add(1)
		d := deleteMe

		go func() {
			defer wg.Done()
			err := s.Delete(d)
			test.AssertNil(t, err)
		}()
	}
	wg.Wait()

	for _, v := range pairs {
		if _, ok := toDelete[v.key]; ok {
			val, ok := s.Get(v.key)
			test.AssertEqual(t, []byte(nil), val)
			test.AssertEqual(t, false, ok)
		} else {
			val, ok := s.Get(v.key)
			test.AssertEqual(t, true, ok)
			test.AssertEqual(t, v.val, val)
		}
	}

	test.AssertNil(t, s.vacuum())
	test.AssertNil(t, s.Close())

	got, err := os.ReadFile(fname)
	test.AssertNil(t, err)

	test.AssertEqual(t, expected.Bytes(), got)
}

// TestSnapshot ensures that Snapshot accurately snapshots Storage.
func TestSnapshot(t *testing.T) {
	testA := filepath.Join(t.TempDir(), "testA")
	s, err := NewStorage(testA, 0644, nil)
	test.AssertNil(t, err)
	kvs := []struct {
		k        string
		v        []byte
		toDelete bool
	}{
		{
			k: "hamfast",
			v: []byte("All's well that ends better."),
		},
		{
			k: "gandalf",
			v: []byte("It is a comfort not to be mistaken at all points. Do I not know it only too well!"),
		},
		{
			k:        "gimli",
			v:        []byte("Deep is the abyss that is spanned by Durin’s Bridge, and none has measured it."),
			toDelete: true,
		},
	}

	for _, kv := range kvs {
		err := s.Set(kv.k, kv.v)
		test.AssertNil(t, err)
		if kv.toDelete {
			err = s.Delete(kv.k)
			test.AssertNil(t, err)
		}
	}

	testB := filepath.Join(t.TempDir(), "testB")
	err = s.Snapshot(testB, 0644)
	test.AssertNil(t, err)

	b, err := NewStorage(testB, 0644, nil)
	test.AssertNil(t, err)
	for _, kv := range kvs {
		expVal := kv.v
		expOK := true
		if kv.toDelete {
			expVal = []byte(nil)
			expOK = false
		}
		sval, ok := s.Get(kv.k)
		test.AssertEqual(t, expOK, ok)
		test.AssertEqual(t, expVal, sval)
		bval, ok := b.Get(kv.k)
		test.AssertEqual(t, expOK, ok)
		test.AssertEqual(t, expVal, bval)
	}
}
