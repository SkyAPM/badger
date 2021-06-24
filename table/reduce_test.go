package table

import (
	"math"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/assert"
)

func newReducedSimpleIterator(keys []k, vals []string) y.Iterator {
	k2 := make([][]byte, len(keys))
	v := make([][]byte, len(vals))
	y.AssertTrue(len(keys) == len(vals))
	for i := 0; i < len(keys); i++ {
		k2[i] = y.KeyWithTs([]byte(keys[i].key), keys[i].ts)
		v[i] = []byte(vals[i])
	}
	return &ReducedSimpleIterator{
		SimpleIterator: SimpleIterator{
			keys:     k2,
			vals:     v,
			idx:      -1,
			reversed: false,
		},
	}
}

type ReducedSimpleIterator struct {
	SimpleIterator
}

func (s *ReducedSimpleIterator) Seek(key []byte) {
	s.idx = sort.Search(len(s.keys), func(i int) bool {
		return y.CompareKeys(s.keys[i], key) >= 0
	})
}

type input struct {
	keys []k
	vals []string
}

type want struct {
	keys []k
	vals [][]k
}

type k struct {
	key string
	ts  uint64
}

func TestReducedUniIterator(t *testing.T) {
	tests := []struct {
		name      string
		valueSize int
		input     input
		want      want
		seekKey   []byte
	}{
		{
			name:      "golden path",
			valueSize: math.MaxUint32,
			input: input{
				keys: []k{
					{"k1", 1},
					{"k1", 0},
					{"k2", 0},
					{"k3", 1},
					{"k3", 0},
					{"k4", 0},
					{"k5", 1},
					{"k5", 0},
				},
				vals: []string{
					"KISS",
					"principle",
					"is",
					"keep",
					"it",
					"simple",
					"stupid",
					"!!!",
				},
			},
			want: want{
				keys: []k{
					{"k1", 0},
					{"k2", 0},
					{"k3", 0},
					{"k4", 0},
					{"k5", 0},
				},
				vals: [][]k{
					{{"KISS", 1}, {"principle", 0}},
					{{"is", 0}},
					{{"keep", 1}, {"it", 0}},
					{{"simple", 0}},
					{{"stupid", 1}, {"!!!", 0}},
				},
			},
		},
		{
			name:      "two group",
			valueSize: 25,
			input: input{
				keys: []k{
					{"k1", 0},
					{"k2", 10},
					{"k2", 8},
					{"k2", 7},
					{"k2", 3},
					{"k2", 1},
					{"k3", 0},
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
			want: want{
				keys: []k{
					{"k1", 0},
					{"k2", 7},
					{"k2", 1},
					{"k3", 0},
				},
				vals: [][]k{
					{{"KISS", 0}},
					{{"principle", 10}, {"is", 8}, {"keep", 7}},
					{{"it", 3}, {"simple", 1}},
					{{"stupid", 0}},
				},
			},
		},
		{
			name:      "seek before",
			valueSize: 25,
			seekKey:   y.KeyWithTs([]byte("k2"), 4),
			input: input{
				keys: []k{
					{"k1", 0},
					{"k2", 10},
					{"k2", 8},
					{"k2", 7},
					{"k2", 3},
					{"k2", 1},
					{"k3", 0},
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
			want: want{
				keys: []k{
					{"k2", 1},
					{"k3", 0},
				},
				vals: [][]k{
					{{"it", 3}, {"simple", 1}},
					{{"stupid", 0}},
				},
			},
		},
		{
			name:      "seek internal",
			valueSize: 25,
			seekKey:   y.KeyWithTs([]byte("k2"), 8),
			input: input{
				keys: []k{
					{"k1", 0},
					{"k2", 10},
					{"k2", 8},
					{"k2", 7},
					{"k2", 3},
					{"k2", 1},
					{"k3", 0},
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
			want: want{
				keys: []k{
					{"k2", 3},
					{"k2", 1},
					{"k3", 0},
				},
				vals: [][]k{
					{{"is", 8}, {"keep", 7}, {"it", 3}},
					{{"simple", 1}},
					{{"stupid", 0}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := NewReducedUniIterator(newReducedSimpleIterator(tt.input.keys, tt.input.vals), 3, tt.valueSize)
			if tt.seekKey != nil {
				iter.Seek(tt.seekKey)
				assert.Equal(t, tt.want, get(t, iter))
				return
			}
			iter.Rewind()
			assert.Equal(t, tt.want, get(t, iter))
			iter.Rewind()
			assert.Equal(t, tt.want, get(t, iter))
			closeAndCheck(t, iter, 1)
		})
	}
}

func get(t *testing.T, iter y.Iterator) (got want) {
	for ; iter.Valid(); iter.Next() {
		got.keys = append(got.keys, k{
			key: string(y.ParseKey(iter.Key())),
			ts:  y.ParseTs(iter.Key()),
		})
		rVal := NewReducedValue(0, 0)
		assert.NoError(t, rVal.Unmarshal(iter.Value().Value))
		iterator := rVal.Iter(false)
		kk := make([]k, 0, rVal.Len())
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			kk = append(kk, k{key: string(iterator.Value().Value), ts: BytesToUint64(iterator.Key())})
		}
		got.vals = append(got.vals, kk)
	}
	return
}

func TestReducedValue(t *testing.T) {
	rVal := NewReducedValue(0, 0)
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
	rVal = NewReducedValue(0, 0)
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
