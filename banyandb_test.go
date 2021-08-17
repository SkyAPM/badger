package badger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v3/y"
)

func TestWriteViaIterator(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}
	opt := DefaultOptions("")
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		s := db.NewSkiplist()
		for i := 0; i < 100; i++ {
			s.Put(y.KeyWithTs(key(i), 101), y.ValueStruct{Value: val(i)})
		}

		// Hand over iterator to Badger.
		require.NoError(t, db.HandoverIterator(s.NewUniIterator(false)))

		// Read the data back.
		itr := db.NewIterator(DefaultIteratorOptions)
		defer itr.Close()

		i := 0
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Value()
			require.Equal(t, string(key(i)), string(y.ParseKey(itr.Key())))
			require.Equal(t, y.ParseTs(itr.Key()), uint64(101))
			require.Equal(t, val(i), item.Value)
			i++
		}
		require.Equal(t, 100, i)
	})
}

func TestPutAndGet(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}
	opt := DefaultOptions("")
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			require.NoError(t, db.Put(y.KeyWithTs(key(i), 101), val(i)))
		}

		// Read the data back.
		itr := db.NewIterator(DefaultIteratorOptions)
		defer itr.Close()

		i := 0
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Value()
			require.Equal(t, string(key(i)), string(y.ParseKey(itr.Key())))
			require.Equal(t, uint64(101), y.ParseTs(itr.Key()))
			require.Equal(t, val(i), item.Value)
			valFromGet, err := db.Get(itr.Key())
			require.NoError(t, err)
			require.Equal(t, val(i), valFromGet.Value)
			require.Equal(t, uint64(101), valFromGet.Version)
			i++
		}
		require.Equal(t, 100, i)
	})
}

func TestPrefix(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%010d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%0128d", i))
	}
	opt := DefaultOptions("")
	runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
		for i := 0; i < 100; i++ {
			require.NoError(t, db.Put(y.KeyWithTs(key(i), 101), val(i)))
		}

		opts := DefaultIteratorOptions
		opts.Prefix = []byte("000000003")
		itr := db.NewIterator(opts)
		defer itr.Close()

		i := 30
		for itr.Seek(nil); itr.Valid(); itr.Next() {
			item := itr.Value()
			require.Equal(t, string(key(i)), string(y.ParseKey(itr.Key())))
			require.Equal(t, uint64(101), y.ParseTs(itr.Key()))
			require.Equal(t, val(i), item.Value)
			valFromGet, err := db.Get(itr.Key())
			require.NoError(t, err)
			require.Equal(t, val(i), valFromGet.Value)
			require.Equal(t, uint64(101), valFromGet.Version)
			i++
		}
		require.Equal(t, 40, i)
	})
}
