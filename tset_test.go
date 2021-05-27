package badger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"reflect"
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
				{[]byte("k3"), 3, []byte{95}},
				{[]byte("k5"), 2, []byte{44}},
				{[]byte("k5"), 1, []byte{66}},
				{[]byte("k4"), 1, []byte{72}},
				{[]byte("k4"), 2, []byte{53}},
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
				{[]byte("k1"), 1, errVal},
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
	splitFunc := func(raw []byte) ([][]byte, error) {
		if raw == nil || len(raw) < 1 {
			return nil, fmt.Errorf("can not find data")
		}
		if bytes.Equal(raw, errVal) {
			return nil, fmt.Errorf("invalid value")
		}
		bb := make([][]byte, len(raw))
		for i := range raw {
			bb[i] = []byte{raw[i]}
		}
		return bb, nil
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dir string
			runTSetTest(t, "", func(t *testing.T, db *DB) {
				s := NewTSet(db, reduceFunc, extractFunc, splitFunc)
				dir = db.opt.Dir

				for _, arg := range tt.args {
					if err := s.Put(arg.key, arg.val, arg.ts); (err != nil) != tt.wantPutErr {
						t.Errorf("Put() error = %v, wantPutErr %v", err, tt.wantPutErr)
					}
				}
				if !tt.wantVLGetErr {
					verifyGet(t, tt, s, tt.wantMTGetErr)
				}
			})
			defer removeDir(dir)
			runTSetTest(t, dir, func(t *testing.T, db *DB) {
				verifyGet(t, tt, NewTSet(db, reduceFunc, extractFunc, splitFunc), tt.wantVLGetErr)
			})
		})
	}
}

func verifyGet(t *testing.T, tt test, s *TSet, wantErr bool) {
	if tt.wantPutErr {
		return
	}
	keyMerge := make(map[string][][]byte, 0)
	for _, arg := range tt.args {
		gotVal, err := s.Get(arg.key, arg.ts)
		if (err != nil) != wantErr {
			t.Errorf("Get() error = %v, wantGetErr %v", err, wantErr)
			return
		}

		if !wantErr && !reflect.DeepEqual(gotVal, arg.val) {
			t.Errorf("Get() gotVal = %v, want %v", gotVal, arg.val)
		}
		if _, ok := keyMerge[string(arg.key)]; !ok {
			keyMerge[string(arg.key)] = make([][]byte, 3)
		}
		keyMerge[string(arg.key)][3-arg.ts] = gotVal
	}
	for key, vals := range keyMerge {
		flattenVals := make([][]byte, 0)
		for _, v := range vals {
			if len(v) > 0 {
				flattenVals = append(flattenVals, v)
			}
		}
		gotVals, err := s.GetAll([]byte(key))
		if (err != nil) != wantErr {
			t.Errorf("GetAll() error = %v, wantGetErr %v", err, wantErr)
			return
		}
		if !wantErr && !reflect.DeepEqual(gotVals, flattenVals) {
			t.Errorf("GetAll() gotVal = %v, want %v", gotVals, flattenVals)
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
