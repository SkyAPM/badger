package badger

import (
	"bytes"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/pkg/errors"
)

var ErrTSetInvalidTS = errors.New("Timestamp should be greater than 0")

type ExtractFunc func(raw []byte, ts uint64) ([]byte, error)

type SplitFunc func(raw []byte) ([][]byte, error)

// TSet is a time-series set, which leverages internal version to store the timestamp. The effect-side is there's no
// chance to append a 8-bit value to the key as the version.
// Once MemTables are flushed, the table.ReduceFunc is invoked to reduce/merge values which have identical keys. In contrast,
// the ExtractFunc helps Get method to extract reduced values from the "vault" created by table.ReduceFunc.
// TSet also provide GetAll which needs a SplitFunc to retrieve all values in the same key.
type TSet struct {
	db          *DB
	extractFunc ExtractFunc
	splitFunc   SplitFunc
}

func NewTSet(db *DB, reduceFunc table.ReduceFunc, extractFunc ExtractFunc, splitFunc SplitFunc) *TSet {
	db.reduceFunc = reduceFunc
	return &TSet{
		db:          db,
		extractFunc: extractFunc,
		splitFunc:   splitFunc,
	}
}

func (s *TSet) Put(key, val []byte, ts uint64) error {
	req, err := s.write(key, val, ts)
	if err != nil {
		return err
	}
	return req.Wait()
}

func (s *TSet) PutAsync(key, val []byte, ts uint64, f func(error)) error {
	req, err := s.write(key, val, ts)
	if err != nil {
		return err
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

func (s *TSet) write(key, val []byte, ts uint64) (*request, error) {
	if ts < 1 {
		return nil, ErrTSetInvalidTS
	}
	entry := &Entry{
		Key:   y.KeyWithTs(key, ts),
		Value: val,
	}
	req, err := s.db.sendToWriteCh([]*Entry{entry})
	if err != nil {
		return nil, fmt.Errorf("failed to send entry to write channel: %v", err)
	}
	return req, nil
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

func (s *TSet) GetAll(key []byte) (val [][]byte, err error) {
	db := s.db
	if db.IsClosed() {
		return nil, ErrDBClosed
	}
	val = s.seekMemTables(key)
	if val != nil && len(val) > 0 {
		return val, nil
	}
	vs, errLC := s.db.lc.get(y.KeyWithTs(key, math.MaxUint64), y.ValueStruct{}, 0)
	if errLC != nil {
		return nil, fmt.Errorf("faliled to get val from leved files: %v", errLC)
	}
	return s.splitFunc(vs.Value)
}

func (s *TSet) seekMemTables(prefix []byte) [][]byte {
	tables, decr := s.db.getMemTables()
	defer decr()
	s.db.vlog.incrIteratorCount()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].sl.NewUniIterator(false))
	}
	it := table.NewMergeIterator(iters, false)
	defer it.Close()
	r := make([][]byte, 0)
	for it.Seek(y.KeyWithTs(prefix, math.MaxUint64)); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		r = append(r, it.Value().Value)
	}
	return r
}
