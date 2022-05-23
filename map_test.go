package bugfruit

import (
	"testing"

	"github.com/reesporte/bugfruit/test"
)

func TestMuMap(t *testing.T) {
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
			k: "boromir",
			v: []byte("You are no Elf."),
		},
	}

	m := newMuMap()
	for _, kv := range kvs {
		d := newDatum()
		err := d.Set(kv.k, kv.v)
		test.AssertNil(t, err)

		m.Store(kv.k, d)

		got, ok := m.Load(kv.k)
		test.AssertEqual(t, true, ok)
		test.AssertEqual(t, d, got)

		got, ok = m.LoadAndDelete(kv.k)
		test.AssertEqual(t, true, ok)
		test.AssertEqual(t, d, got)

		got, ok = m.Load(kv.k)
		test.AssertEqual(t, false, ok)
		d = nil
		test.AssertEqual(t, d, got)
	}
}
