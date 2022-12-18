package table

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3/banyandb"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/require"
)

type tuple struct {
	key string
	val int64
	ts  uint64
}

func buildIntegerTable(t *testing.T, keyValues []tuple, opts Options) *Table {
	b := NewTableBuilder(opts)
	defer b.Close()
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())

	sort.Slice(keyValues, func(i, j int) bool {
		if keyValues[i].key == keyValues[j].key {
			return keyValues[i].ts > keyValues[j].ts
		}
		return keyValues[i].key < keyValues[j].key
	})
	for _, kv := range keyValues {
		b.Add(y.KeyWithTs([]byte(kv.key), kv.ts),
			y.ValueStruct{Value: y.I64ToBytes(kv.val)}, 0)
	}
	tbl, err := CreateTable(filename, b)
	require.NoError(t, err, "writing to file failed")
	return tbl
}

func TestEncodingMergingIterator(t *testing.T) {
	opts := Options{
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 15,
		BlockSize:            4 * 1024,
		BloomFalsePositive:   0.01,
		SameKeyInBlock:       true,
		EncoderPool: banyandb.NewIntEncoderPool("test", 2, func(key []byte) time.Duration {
			return time.Second
		}),
		DecoderPool: banyandb.NewIntDecoderPool("test", 2, func(key []byte) time.Duration {
			return time.Second
		}),
	}
	tbl1 := buildIntegerTable(t, []tuple{
		{"k1", 1, uint64(3 * time.Second)},
		{"k1", 3, uint64(1 * time.Second)},
		{"k1", 4, uint64(0 * time.Second)},
		{"k3", 6, uint64(0 * time.Second)},
	}, opts)
	tbl2 := buildIntegerTable(t, []tuple{
		{"k1", 2, uint64(2 * time.Second)},
		{"k2", 5, uint64(0 * time.Second)},
	}, opts)

	expected := []tuple{
		{"k1", 1, uint64(3 * time.Second)},
		{"k1", 2, uint64(2 * time.Second)},
		{"k1", 3, uint64(1 * time.Second)},
		{"k1", 4, uint64(0 * time.Second)},
		{"k2", 5, uint64(0 * time.Second)},
		{"k3", 6, uint64(0 * time.Second)},
	}
	defer tbl1.DecrRef()
	defer tbl2.DecrRef()
	it1 := tbl1.NewIterator(0)
	it2 := NewConcatIterator([]*Table{tbl2}, 0)
	it := NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	var i int
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Key()
		vs := it.Value()
		fmt.Printf("%s %d %d\n", y.ParseKey(k), y.ParseTs(k), y.BytesToI64(vs.Value))
		require.EqualValues(t, expected[i].key, string(y.ParseKey(k)))
		require.EqualValues(t, expected[i].val, y.BytesToI64(vs.Value))
		i++
	}
	require.Equal(t, i, len(expected))
	require.False(t, it.Valid())
}

func TestSameKeyBlock(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := Options{
				Compression:          options.ZSTD,
				ZSTDCompressionLevel: 15,
				BlockSize:            4 * 1024,
				BloomFalsePositive:   0.01,
				SameKeyInBlock:       true,
			}
			table := buildTestTable(t, "key", n, opts)
			defer table.DecrRef()
			it := table.NewIterator(0)
			defer it.Close()
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				v := it.Value()
				k := y.KeyWithTs([]byte(key("key", count)), 0)
				require.EqualValues(t, k, it.Key())
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				count++
			}
			require.Equal(t, count, n)
		})
	}
}
