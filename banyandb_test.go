package badger

import (
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v3/banyandb"
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

func TestTSetGet(t *testing.T) {
	now := time.Now()
	oneMinuteLater := now.Add(time.Minute)
	twoMinuteLater := oneMinuteLater.Add(time.Minute)
	inputData := []arg{
		{"k1", now, 1},
		{"k2", now, 12},
		{"k3", now, 83},
		{"k5", now, 66},
		{"k4", now, 72},
		{"k1", oneMinuteLater, 2},
		{"k3", oneMinuteLater, 72},
		{"k5", oneMinuteLater, 44},
		{"k4", oneMinuteLater, 53},
		{"k3", twoMinuteLater, 95},
	}
	tests := []test{
		{
			name:      "golden path",
			valueSize: math.MaxInt64,
			args:      inputData,
		},
		{
			name:      "medium window",
			valueSize: 10,
			args:      inputData,
		},
		{
			name:      "small window",
			valueSize: 2,
			args:      inputData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTest(t, tt, verifyGet)
			runTest2(t, tt, verifyGet)
		})
	}
}

type arg struct {
	key string
	ts  time.Time
	val int64
}

type test struct {
	name         string
	args         []arg
	valueSize    int
	wantPutErr   bool
	wantMTGetErr bool
	wantVLGetErr bool
}

type verifyFunc func(t *testing.T, tt test, s *TSet, wantErr bool)

func runTest(t *testing.T, tt test, f verifyFunc) {
	var dir string
	// Verify memory
	runTSetTest(t, "", tt.valueSize, func(t *testing.T, db *DB) {
		s := NewTSet(db)
		dir = db.opt.Dir

		for _, arg := range tt.args {
			if err := s.Put([]byte(arg.key), y.I64ToBytes(arg.val), uint64(arg.ts.UnixNano())); (err != nil) != tt.wantPutErr {
				t.Errorf("Put() error = %v, wantPutErr %v", err, tt.wantPutErr)
			}
		}
		if !tt.wantVLGetErr {
			f(t, tt, s, tt.wantMTGetErr)
		}
	})
	defer removeDir(dir)
	// Verify disk tables
	runTSetTest(t, dir, tt.valueSize, func(t *testing.T, db *DB) {
		f(t, tt, NewTSet(db), tt.wantVLGetErr)
	})
}

func runTest2(t *testing.T, tt test, f verifyFunc) {
	var dir string
	// Verify memory + two L0 tables
	runTSetTest(t, "", tt.valueSize, func(t *testing.T, db *DB) {
		s := NewTSet(db)
		dir = db.opt.Dir

		var ts int64
		for _, arg := range tt.args {
			tn := arg.ts.UnixNano()
			if ts == 0 {
				ts = tn
			} else if ts != tn {
				db.lock.Lock()
				var wg sync.WaitGroup
				wg.Add(1)
				db.flushChan <- flushTask{mt: db.mt, cb: func() {
					wg.Done()
				}}
				// We manage to push this task. Let's modify imm.
				db.imm = append(db.imm, db.mt)
				var err error
				db.mt, err = db.newMemTable()
				db.lock.Unlock()
				require.NoError(t, err)
				wg.Wait()
				ts = tn
			}
			if err := s.Put([]byte(arg.key), y.I64ToBytes(arg.val), uint64(tn)); (err != nil) != tt.wantPutErr {
				t.Errorf("Put() error = %v, wantPutErr %v", err, tt.wantPutErr)
			}
		}
		if !tt.wantVLGetErr {
			f(t, tt, s, tt.wantMTGetErr)
		}
	})
	defer removeDir(dir)
}

func verifyGet(t *testing.T, tt test, s *TSet, wantErr bool) {
	if tt.wantPutErr {
		return
	}
	for _, arg := range tt.args {
		gotVal, err := s.Get([]byte(arg.key), uint64(arg.ts.UnixNano()))
		if (err != nil) != wantErr {
			t.Errorf("Get() error = %v, wantGetErr %v", err, wantErr)
			return
		}

		if !wantErr && !reflect.DeepEqual(y.BytesToI64(gotVal), arg.val) {
			t.Errorf("Get() gotVal = %v, want %v", gotVal, arg.val)
		}
	}
}

func runTSetTest(t *testing.T, dir string, valueSize int, test func(t *testing.T, db *DB)) {
	if dir == "" {
		d, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		dir = d
	}
	opts := getTestOptions(dir)
	opts = opts.WithKeyBasedEncoder(
		banyandb.NewIntEncoderPool("test", valueSize, func(key []byte) time.Duration {
			return time.Minute
		}),
		banyandb.NewIntDecoderPool("test", valueSize, func(key []byte) time.Duration {
			return time.Minute
		}),
	)

	db, err := Open(opts)

	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	test(t, db)
}
