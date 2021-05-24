package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/require"
)

var errVal = []byte{121}

type arg struct {
	key []byte
	ts  uint64
	val []byte
}

type test struct {
	name         string
	args         []arg
	wantPutErr   bool
	wantMTGetErr bool
	wantVLGetErr bool
}

func TestNewTSet(t *testing.T) {
	tests := []test{
		{
			name: "golden path",
			args: []arg{
				{[]byte("k1"), 1, []byte{1}},
				{[]byte("k1"), 2, []byte{2}},
				{[]byte("k2"), 1, []byte{12}},
				{[]byte("k3"), 2, []byte{72}},
				{[]byte("k3"), 1, []byte{83}},
			},
		},
		{
			name: "invalid ts",
			args: []arg{
				{[]byte("k1"), 0, []byte{1}},
			},
			wantPutErr: true,
		},
		{
			name: "invalid value",
			args: []arg{
				{[]byte("k1"), math.MaxUint64, errVal},
			},
			wantVLGetErr: true,
		},
	}
	reduceFunc := func(left bytes.Buffer, right y.ValueStruct) bytes.Buffer {
		if right.Value == nil {
			return left
		}
		left.Write(right.Value[:1])
		return left
	}
	extractFunc := func(raw []byte, ts uint64) ([]byte, error) {
		if raw == nil || len(raw) < 1 {
			return nil, fmt.Errorf("can not find data")
		}
		if bytes.Equal(raw, errVal) {
			return nil, fmt.Errorf("invalid value")
		}
		return []byte{raw[len(raw)-int(ts)]}, nil
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dir string
			runTSetTest(t, "", func(t *testing.T, db *DB) {
				s := NewTSet(db, reduceFunc, extractFunc)
				dir = db.opt.Dir

				var wg sync.WaitGroup
				for _, arg := range tt.args {
					if !tt.wantPutErr {
						wg.Add(1)
					}
					if err := s.Put(arg.key, arg.val, arg.ts, func(err error) {
						wg.Done()
						if (err != nil) != tt.wantPutErr {
							t.Errorf("Put() error = %v, wantPutErr %v", err, tt.wantPutErr)
						}
					}); (err != nil) != tt.wantPutErr {
						t.Errorf("Put() error = %v, wantPutErr %v", err, tt.wantPutErr)
					}
				}
				wg.Wait()
				if !tt.wantVLGetErr {
					verifyGet(t, tt, s, tt.wantMTGetErr)
				}
			})
			defer removeDir(dir)
			runTSetTest(t, dir, func(t *testing.T, db *DB) {
				verifyGet(t, tt, NewTSet(db, reduceFunc, extractFunc), tt.wantVLGetErr)
			})
		})
	}
}

func verifyGet(t *testing.T, tt test, s *TSet, wantErr bool) {
	if tt.wantPutErr {
		return
	}
	for _, arg := range tt.args {
		gotVal, err := s.Get(arg.key, arg.ts)
		if (err != nil) != wantErr {
			t.Errorf("Get() error = %v, wantGetErr %v", err, wantErr)
			return
		}
		if !wantErr && !reflect.DeepEqual(gotVal, arg.val) {
			t.Errorf("Get() gotVal = %v, want %v", gotVal, arg.val)
		}
	}
}

func runTSetTest(t *testing.T, dir string, test func(t *testing.T, db *DB)) {
	if dir == "" {
		d, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		dir = d
	}
	opts := new(Options)
	*opts = getTestOptions(dir)

	db, err := Open(*opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	test(t, db)
}
