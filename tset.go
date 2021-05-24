package badger

import (
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/pkg/errors"
)

var ErrTSetInvalidTS = errors.New("Timestamp should be greater than 0")

type ExtractFunc func(raw []byte, ts uint64) ([]byte, error)

// TSet is a time-series set, which leverage internal version to store timestamp. The effect-side is you never append a
// 8-bit value to the key as the version.
// Once MemTables are flushed, the table.ReduceFunc is invoked to reduce/merge values with identical keys. In contrast,
// the ExtractFunc helps Get method to extract reduced values from the "vault" created by table.ReduceFunc
type TSet struct {
	db          *DB
	extractFunc ExtractFunc
}

func NewTSet(db *DB, reduceFunc table.ReduceFunc, extractFunc ExtractFunc) *TSet {
	db.reduceFunc = reduceFunc
	return &TSet{
		db:          db,
		extractFunc: extractFunc,
	}
}

func (s *TSet) Put(key, val []byte, ts uint64, f func(error)) error {
	if ts < 1 {
		return ErrTSetInvalidTS
	}
	entry := &Entry{
		Key:   y.KeyWithTs(key, ts),
		Value: val,
	}
	req, err := s.db.sendToWriteCh([]*Entry{entry})
	if err != nil {
		return fmt.Errorf("failed to send entry to write channel: %v", err)
	}
	if f == nil {
		return nil
	}
	go func() {
		err := req.Wait()
		f(err)
	}()
	return nil
}

func (s *TSet) Get(key []byte, ts uint64) (val []byte, err error) {
	db := s.db
	if db.IsClosed() {
		return nil, ErrDBClosed
	}
	tables, decr := db.getMemTables() // Lock should be released.
	defer decr()

	version := ts

	y.NumGetsAdd(db.opt.MetricsEnabled, 1)
	for i := 0; i < len(tables); i++ {
		vs := tables[i].sl.Get(y.KeyWithTs(key, ts))
		y.NumMemtableGetsAdd(db.opt.MetricsEnabled, 1)
		if vs.Meta == 0 && vs.Value == nil {
			continue
		}
		if vs.Version == version {
			return vs.Value, nil
		}
	}
	vs, errLC := s.db.lc.get(y.KeyWithTs(key, math.MaxUint64), y.ValueStruct{}, 0)
	if errLC != nil {
		return nil, fmt.Errorf("faliled to get val from leved files: %v", errLC)
	}
	return s.extractFunc(vs.Value, ts)
}
