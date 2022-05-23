package bugfruit

import (
	"bytes"
)

// datum represents a key value pair and its metadata.
type datum struct {
	meta  *meta
	key   string
	value []byte
	idx   uint32
}

// newDatum instantiates a new datum
func newDatum() *datum {
	return &datum{meta: &meta{}}
}

// Set sets the key and value for a datum as well as its associated metadata.
func (d *datum) Set(key string, value []byte) error {
	if !bytes.Equal(value, d.value) {
		d.meta.valSize = uint32(len(value))
		d.value = value
	}
	if key != d.key {
		d.meta.keySize = uint32(len([]byte(key)))
		d.key = key
	}
	return nil
}

// Clone returns a deep copy of a datum.
func (d *datum) Clone() *datum {
	newD := newDatum()
	newD.Set(d.key, d.value)
	newD.idx = d.idx
	return newD
}

// MarkDeleted marks a datum as deleted
func (d *datum) MarkDeleted() {
	d.meta.deleted = byte(1)
}

// Deleted returns the deleted value of a datum's meta.
func (d *datum) Deleted() byte {
	return d.meta.deleted
}

// Bytes converts a datum struct to a byte slice for writing to file.
func (d *datum) Bytes() []byte {
	b := make([]byte, d.Size())
	// add the metadata
	copy(b[:metaSize], d.meta.Bytes())

	// add the key
	copy(b[metaSize:metaSize+d.meta.keySize], []byte(d.key))

	// add the value
	copy(b[metaSize+d.meta.keySize:], []byte(d.value))

	return b
}

// KeyValFromBytes converts a byte slice to a key/value pair and saves it to the
// datum. It returns an error if the length of the byte slice does not equal the
// keySize plus the valSize.
func (d *datum) KeyValFromBytes(b []byte) (err error) {
	if d.meta == nil {
		return ErrNoMetadata
	}

	if uint32(len(b)) != d.meta.keySize+d.meta.valSize {
		return ErrInvalidKeyValSlice
	}

	d.key = string(b[:d.meta.keySize])
	d.value = b[d.meta.keySize : d.meta.keySize+d.meta.valSize]
	return nil
}

// Size returns the size of the datum when written to file in bytes.
func (d *datum) Size() uint32 {
	return d.meta.keySize + d.meta.valSize + metaSize
}
