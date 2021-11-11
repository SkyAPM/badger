package badger

import (
	"bytes"
	"io/ioutil"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v3/bydb"
)

type arg struct {
	key []byte
	ts  uint64
	val []byte
}

type test struct {
	name         string
	args         []arg
	valueSize    int
	wantPutErr   bool
	wantMTGetErr bool
	wantVLGetErr bool
}

func TestTSetGet(t *testing.T) {
	inputData := []arg{
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
		})
	}
}

type verifyFunc func(t *testing.T, tt test, s *TSet, wantErr bool)

func runTest(t *testing.T, tt test, f verifyFunc) {
	var dir string
	encoderFactory := func() bydb.TSetEncoder {
		return bydb.NewBlockEncoder(tt.valueSize)
	}
	decoderFactory := func() bydb.TSetDecoder {
		return new(bydb.BlockDecoder)
	}
	runTSetTest(t, "", func(t *testing.T, db *DB) {
		s := NewTSet(db, WithEncoderFactory(encoderFactory), WithDecoderFactory(decoderFactory))
		dir = db.opt.Dir

		for _, arg := range tt.args {
			if err := s.Put(arg.key, arg.val, arg.ts); (err != nil) != tt.wantPutErr {
				t.Errorf("Put() error = %v, wantPutErr %v", err, tt.wantPutErr)
			}
		}
		if !tt.wantVLGetErr {
			f(t, tt, s, tt.wantMTGetErr)
		}
	})
	defer removeDir(dir)
	runTSetTest(t, dir, func(t *testing.T, db *DB) {
		f(t, tt, NewTSet(db, WithEncoderFactory(encoderFactory), WithDecoderFactory(decoderFactory)), tt.wantVLGetErr)
	})
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

func TestTSetGetALL(t *testing.T) {
	inputData := []arg{
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
	}
	tests := []test{
		{
			name:      "golden path",
			valueSize: math.MaxInt64,
			args:      inputData,
		},
		{
			name:      "minimal value size",
			valueSize: 11,
			args:      inputData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTest(t, tt, verifyGetALL)
		})
	}
}

func verifyGetALL(t *testing.T, tt test, s *TSet, err bool) {
	mergedKeys := make([][]byte, 0)
	mergedVals := make([][][]byte, 0)
	for _, arg := range tt.args {
		if len(mergedKeys) > 0 && bytes.Equal(mergedKeys[len(mergedKeys)-1], arg.key) {
			v := mergedVals[len(mergedVals)-1]
			v[3-arg.ts] = arg.val
			mergedVals[len(mergedVals)-1] = v
			continue
		}
		mergedKeys = append(mergedKeys, arg.key)
		val := make([][]byte, 3)
		val[3-arg.ts] = arg.val
		mergedVals = append(mergedVals, val)
	}
	for i, mergedKey := range mergedKeys {
		got, err := s.GetAll(mergedKey)
		assert.NoError(t, err)
		want := make([][]byte, 0)
		for _, v := range mergedVals[i] {
			if v == nil {
				continue
			}
			want = append(want, v)
		}
		assert.Equal(t, want, got)
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
