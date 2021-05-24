package table

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v3/y"
)

func newReducedSimpleIterator(keys [][]byte, vals []string) *SimpleIterator {
	k := make([][]byte, len(keys))
	v := make([][]byte, len(vals))
	y.AssertTrue(len(keys) == len(vals))
	for i := 0; i < len(keys); i++ {
		k[i] = keys[i]
		v[i] = []byte(vals[i])
	}
	return &SimpleIterator{
		keys:     k,
		vals:     v,
		idx:      -1,
		reversed: false,
	}
}

type kv struct {
	keys [][]byte
	vals []string
}

func TestReducedUniIterator(t *testing.T) {
	tests := []struct {
		name    string
		input   kv
		want    kv
		seekKey []byte
	}{
		{
			name: "golden path",
			input: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k2"), 1),
					y.KeyWithTs([]byte("k3"), 0),
					y.KeyWithTs([]byte("k4"), 0),
					y.KeyWithTs([]byte("k4"), 1),
					y.KeyWithTs([]byte("k5"), 0),
				},
				vals: []string{
					"KISS",
					"principle",
					"is",
					"keep",
					"it",
					"simple",
					"stupid",
				},
			},
			want: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 1),
					y.KeyWithTs([]byte("k3"), 0),
					y.KeyWithTs([]byte("k4"), 1),
					y.KeyWithTs([]byte("k5"), 0),
				},
				vals: []string{
					"KISS0 !!!",
					"principle0 is1 !!!",
					"keep0 !!!",
					"it0 simple1 !!!",
					"stupid0 !!!",
				},
			},
		},
		{
			name: "head",
			input: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k1"), 1),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k3"), 0),
				},
				vals: []string{
					"keep",
					"it",
					"simple",
					"stupid",
				},
			},
			want: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 1),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k3"), 0),
				},
				vals: []string{
					"keep0 it1 !!!",
					"simple0 !!!",
					"stupid0 !!!",
				},
			},
		},
		{
			name: "tail",
			input: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k3"), 0),
					y.KeyWithTs([]byte("k3"), 1),
				},
				vals: []string{
					"keep",
					"it",
					"simple",
					"stupid",
				},
			},
			want: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k3"), 1),
				},
				vals: []string{
					"keep0 !!!",
					"it0 !!!",
					"simple0 stupid1 !!!",
				},
			},
		},
		{
			name:    "seek",
			seekKey: y.KeyWithTs([]byte("k1"), 1),
			input: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k2"), 1),
					y.KeyWithTs([]byte("k3"), 0),
				},
				vals: []string{
					"keep",
					"it",
					"simple",
					"stupid",
				},
			},
			want: kv{
				keys: [][]byte{
					y.KeyWithTs([]byte("k2"), 1),
					y.KeyWithTs([]byte("k3"), 0),
				},
				vals: []string{
					"it0 simple1 !!!",
					"stupid0 !!!",
				},
			},
		},
	}
	reducer := func(left bytes.Buffer, right y.ValueStruct) bytes.Buffer {
		if right.Value == nil {
			left.Write([]byte("!!!"))
			return left
		}
		left.Write(right.Value)
		left.Write([]byte(strconv.FormatUint(right.Version, 10)))
		left.Write([]byte(" "))
		return left
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := NewReducedUniIterator(newReducedSimpleIterator(tt.input.keys, tt.input.vals), reducer)
			if tt.seekKey != nil {
				iter.Seek(tt.seekKey)
				got := get(iter)
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Seek() = %v, want %v", got, tt.want)
				}
				return
			}
			iter.Rewind()
			got := get(iter)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Next() = %v, want %v", got, tt.want)
			}
			iter.Rewind()
			got = get(iter)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Rewind() = %v, want %v", got, tt.want)
			}
			closeAndCheck(t, iter, 1)
		})
	}
}

func get(iter y.Iterator) (got kv) {
	for ; iter.Valid(); iter.Next() {
		got.keys = append(got.keys, iter.Key())
		got.vals = append(got.vals, string(iter.Value().Value))
	}
	return
}
