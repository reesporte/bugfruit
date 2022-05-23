package bugfruit

import (
	"testing"

	"github.com/reesporte/bugfruit/test"
)

// TestMeta ensures converting meta to and from byte slices works.
func TestMeta(t *testing.T) {
	// converting to bytes
	there := &meta{keySize: 8675309, valSize: 10, deleted: 1}
	bytes := there.Bytes()
	expected := []byte{0xed, 0x5f, 0x84, 0x0, 0xa, 0x0, 0x0, 0x0, 0x1}
	test.AssertEqual(t, expected, bytes)

	// and back again
	back := &meta{}
	err := back.FromBytes(bytes)
	test.AssertNil(t, err)
	test.AssertEqual(t, there, back)

	// convert a bad slice into meta
	bad := &meta{}
	err = bad.FromBytes([]byte{0x62, 0x61, 0x64})
	got, exp := err.Error(), ErrInvalidMetaSlice.Error()
	test.AssertEqual(t, exp, got)
}
