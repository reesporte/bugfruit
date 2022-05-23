package bugfruit

import (
	"testing"

	"github.com/reesporte/bugfruit/test"
)

// TestNewDatum ensures that newDatum instantiates a new datum properly.
func TestNewDatum(t *testing.T) {
	d := newDatum()
	test.AssertNotEqual(t, (*meta)(nil), d.meta)
}

// TestSetDatum ensures that Setting values on a datum updates meta and the values
// properly.
func TestSetDatum(t *testing.T) {
	d := newDatum()

	k := "heck"
	v := []byte("yeah")
	err := d.Set(k, v)
	test.AssertNil(t, err)

	test.AssertEqual(t, d.meta.keySize, uint32(len(k)))
	test.AssertEqual(t, d.meta.valSize, uint32(len(v)))
	test.AssertEqual(t, d.key, k)
	test.AssertEqual(t, d.value, v)
}

// TestCloneDatum ensures that cloning a datum works properly.
func TestCloneDatum(t *testing.T) {
	d := newDatum()

	k := "heck"
	v := []byte("yeah")
	err := d.Set(k, v)
	test.AssertNil(t, err)

	clone := d.Clone()

	test.AssertEqual(t, d, clone)
}

// TestSize ensures that calling d.Size() and d.ValSize()
// and d.KeySize() and d.unprotectedSize() returns
// the correct size.
func TestSize(t *testing.T) {
	d := newDatum()
	k := "heck"
	v := []byte("yeah")
	err := d.Set(k, v)
	test.AssertNil(t, err)
	keySize := uint32(len(k))
	valSize := uint32(len(v))

	exp := keySize + valSize + metaSize
	test.AssertEqual(t, exp, d.Size())
	test.AssertEqual(t, valSize, d.meta.valSize)
	test.AssertEqual(t, keySize, d.meta.keySize)
}

// TestMarkDeleted ensures that marking a datum as
// deleted actually marks it as deleted.
func TestMarkDeleted(t *testing.T) {
	d := newDatum()
	d.MarkDeleted()

	test.AssertEqual(t, byte(1), d.meta.deleted)
}

// TestDeleted ensures that getting the deleted value
// of a datum's meta is accurate.
func TestDeleted(t *testing.T) {
	d := newDatum()
	test.AssertEqual(t, byte(0), d.Deleted())
	d.MarkDeleted()
	test.AssertEqual(t, byte(1), d.Deleted())
}

// TestDatum ensures creating datums, setting datums, and converting datums to/from
// bytes works.
func TestDatum(t *testing.T) {
	// set a datum
	d := newDatum()
	k, v := "test", []byte("time")
	err := d.Set(k, v)
	test.AssertNil(t, err)
	test.AssertEqual(t, k, d.key)
	test.AssertEqual(t, v, d.value)

	// convert to bytes
	b := d.Bytes()
	test.AssertEqual(t, []byte{0x4, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x74, 0x65, 0x73, 0x74, 0x74, 0x69, 0x6d, 0x65}, b)

	// and back again
	d2 := newDatum()
	test.AssertNil(t, d2.meta.FromBytes(b[0:metaSize]))
	test.AssertNil(t, d2.KeyValFromBytes(b[metaSize:]))
	test.AssertEqual(t, d, d2)

	// wrong size byte slice should err
	test.AssertEqual(t, ErrInvalidKeyValSlice, d2.KeyValFromBytes(b[11:]))

	// nil metadata should err
	d3 := newDatum()
	d3.meta = nil
	test.AssertEqual(t, ErrNoMetadata, d3.KeyValFromBytes(b[metaSize:]))
}
