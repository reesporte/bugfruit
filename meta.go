package bugfruit

import (
	"encoding/binary"
)

const metaSize = 9 // 9 bytes == 2 uint32s plus 1 byte

// meta is the metadata for a given key
type meta struct {
	keySize uint32 // how many bytes does the key span
	valSize uint32 // how many bytes does the data span
	deleted byte   // whether the data is deleted
}

// FromBytes converts a byte slice to a meta struct.
func (m *meta) FromBytes(b []byte) error {
	if len(b) != metaSize {
		return ErrInvalidMetaSlice
	}

	m.keySize = binary.LittleEndian.Uint32(b[0:4])
	m.valSize = binary.LittleEndian.Uint32(b[4:8])
	m.deleted = b[8]
	return nil
}

// Bytes converts a meta struct to a byte slice for writing to file.
func (m *meta) Bytes() []byte {
	b := make([]byte, 9)
	binary.LittleEndian.PutUint32(b[:4], m.keySize)
	binary.LittleEndian.PutUint32(b[4:8], m.valSize)
	b[8] = m.deleted

	return b
}
