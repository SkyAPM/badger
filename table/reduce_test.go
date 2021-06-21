package table

import (
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/assert"
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

type wantedKV struct {
	keys [][]byte
	vals [][]byte
}

type kvVal struct {
	key string
	ts  uint64
}

func val(kvv ...kvVal) []byte {
	result := make([]byte, 0)
	result = append(result, Uint32ToBytes(uint32(len(kvv)))...)
	data := make([]byte, 0)
	index := make([]byte, 0, len(kvv))
	for _, li := range kvv {
		index = append(index, Uint64ToBytes(li.ts)...)
		index = append(index, Uint32ToBytes(uint32(len(data)))...)
		lbytes := []byte(li.key)
		data = append(data, Uint32ToBytes(uint32(len(lbytes)))...)
		data = append(data, lbytes...)

	}
	result = append(result, Uint32ToBytes(uint32(len(data)))...)
	result = append(result, data...)
	result = append(result, index...)
	return result
}

func TestReducedUniIterator(t *testing.T) {
	tests := []struct {
		name    string
		input   kv
		want    wantedKV
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
			want: wantedKV{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 1),
					y.KeyWithTs([]byte("k3"), 0),
					y.KeyWithTs([]byte("k4"), 1),
					y.KeyWithTs([]byte("k5"), 0),
				},
				vals: [][]byte{
					val(kvVal{"KISS", 0}),
					val(kvVal{"principle", 0}, kvVal{"is", 1}),
					val(kvVal{"keep", 0}),
					val(kvVal{"it", 0}, kvVal{"simple", 1}),
					val(kvVal{"stupid", 0}),
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
			want: wantedKV{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 1),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k3"), 0),
				},
				vals: [][]byte{
					val(kvVal{"keep", 0}, kvVal{"it", 1}),
					val(kvVal{"simple", 0}),
					val(kvVal{"stupid", 0}),
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
			want: wantedKV{
				keys: [][]byte{
					y.KeyWithTs([]byte("k1"), 0),
					y.KeyWithTs([]byte("k2"), 0),
					y.KeyWithTs([]byte("k3"), 1),
				},
				vals: [][]byte{
					val(kvVal{"keep", 0}),
					val(kvVal{"it", 0}),
					val(kvVal{"simple", 0}, kvVal{"stupid", 1}),
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
			want: wantedKV{
				keys: [][]byte{
					y.KeyWithTs([]byte("k2"), 1),
					y.KeyWithTs([]byte("k3"), 0),
				},
				vals: [][]byte{
					val(kvVal{"it", 0}, kvVal{"simple", 1}),
					val(kvVal{"stupid", 0}),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := NewReducedUniIterator(newReducedSimpleIterator(tt.input.keys, tt.input.vals), 3)
			if tt.seekKey != nil {
				iter.Seek(tt.seekKey)
				got := get(t, iter)
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Seek() = %v, want %v", got, tt.want)
				}
				return
			}
			iter.Rewind()
			got := get(t, iter)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Next() = %v, want %v", got, tt.want)
			}
			iter.Rewind()
			got = get(t, iter)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Rewind() = %v, want %v", got, tt.want)
			}
			closeAndCheck(t, iter, 1)
		})
	}
}

func get(t *testing.T, iter y.Iterator) (got wantedKV) {
	for ; iter.Valid(); iter.Next() {
		got.keys = append(got.keys, iter.Key())
		rawVal := iter.Value().Value
		compressSize := BytesToUint16(rawVal[:2])
		raw, err := y.ZSTDDecompress(make([]byte, 0, compressSize), rawVal[2:])
		if err != nil {
			t.Errorf("failed to decompress: %v", err)
		}
		got.vals = append(got.vals, raw)
	}
	return
}

func TestReducedValue(t *testing.T) {
	rVal := ReducedValue{CompressLevel: 3}
	rVal.Append(y.ValueStruct{
		Value:   []byte("simple"),
		Version: 7,
	})
	rVal.Append(y.ValueStruct{
		Value:   []byte("it"),
		Version: 5,
	})
	rVal.Append(y.ValueStruct{
		Value:   []byte("keep"),
		Version: 0,
	})
	data, err := rVal.Marshal()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
	rVal = ReducedValue{CompressLevel: 3}
	assert.NoError(t, rVal.Unmarshal(data))

	value, errGet := rVal.Get(5)
	assert.NoError(t, errGet)
	assert.Equal(t, []byte("it"), value)
	value, errGet = rVal.Get(3)
	assert.Error(t, errGet)

	iter := rVal.Iter(true)
	expectIter(t, iter, []uint64{0, 5, 7}, []string{"keep", "it", "simple"})
	expectIter(t, iter, []uint64{5, 7}, []string{"it", "simple"}, 1)
	expectIter(t, iter, []uint64{5, 7}, []string{"it", "simple"}, 5)
	expectIter(t, iter, []uint64{7}, []string{"simple"}, 6)
	expectIter(t, iter, []uint64{}, []string{}, 9)

	iter = rVal.Iter(false)
	expectIter(t, iter, []uint64{7, 5, 0}, []string{"simple", "it", "keep"})
	expectIter(t, iter, []uint64{5, 0}, []string{"it", "keep"}, 6)
	expectIter(t, iter, []uint64{5, 0}, []string{"it", "keep"}, 5)
	expectIter(t, iter, []uint64{0}, []string{"keep"}, 1)
}

func expectIter(t *testing.T, iter y.Iterator, wantTss []uint64, wantStrings []string, searchKey ...uint64) {
	tss := make([]uint64, 0)
	strings := make([]string, 0)
	iter.Rewind()
	if len(searchKey) > 0 {
		iter.Seek(Uint64ToBytes(searchKey[0]))
	}
	for ; iter.Valid(); iter.Next() {
		tss = append(tss, BytesToUint64(iter.Key()))
		strings = append(strings, string(iter.Value().Value))
	}
	assert.Equal(t, wantTss, tss)
	assert.Equal(t, wantStrings, strings)
}
