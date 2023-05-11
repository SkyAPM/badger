package badger

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/pkg/errors"
)

// HandoverIterator MUST run serially.
func (db *DB) HandoverIterator(it y.Iterator) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	var entries []*Entry
	for it.Rewind(); it.Valid(); it.Next() {
		v := it.Value()
		e := &Entry{
			Key:       it.Key(),
			Value:     v.Value,
			ExpiresAt: v.ExpiresAt,
			UserMeta:  v.UserMeta,
		}
		entries = append(entries, e)
	}
	req := &request{
		Entries: entries,
	}
	reqs := []*request{req}
	db.pub.sendUpdates(reqs)
	for {
		err := db.handleFlushTask(flushTask{itr: it})
		if err == nil {
			break
		}
		// Encountered error. Retry indefinitely.
		db.opt.Errorf("Failure while flushing iterator to disk: %v. Retrying...\n", err)
		time.Sleep(time.Second)
	}
	return nil
}

func (db *DB) NewIterator(opt IteratorOptions) y.Iterator {
	if db.IsClosed() {
		panic(ErrDBClosed.Error())
	}
	tables, decr := db.getMemTables()
	defer decr()
	db.vlog.incrIteratorCount()
	var iters []y.Iterator
	for i := 0; i < len(tables); i++ {
		iters = append(iters, tables[i].sl.NewUniIterator(opt.Reverse))
	}
	iters = append(iters, db.lc.iterators(&opt)...) // This will increment references.
	return table.NewMergeIterator(iters, opt.Reverse)
}

func (db *DB) Get(key []byte) (y.ValueStruct, error) {
	if len(key) < 1 {
		return y.ValueStruct{}, ErrEmptyKey
	}
	vs, err := db.get(key)
	if err != nil {
		return y.ValueStruct{}, y.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return y.ValueStruct{}, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return y.ValueStruct{}, ErrKeyNotFound
	}
	result := y.ValueStruct{}
	result.Meta = vs.Meta
	result.UserMeta = vs.UserMeta
	result.Version = y.ParseTs(key)
	result.ExpiresAt = vs.ExpiresAt
	result.Value = y.Copy(vs.Value)
	return result, nil
}

func (db *DB) Put(key, val []byte) error {
	req, err := db.write(key, val)
	if err != nil {
		return err
	}
	return req.Wait()
}

func (db *DB) PutAsync(key, val []byte, f func(error)) error {
	req, err := db.write(key, val)
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

func (db *DB) write(key, val []byte) (*request, error) {
	switch {
	case len(key) == 0:
		return nil, ErrEmptyKey
	case bytes.HasPrefix(key, badgerPrefix):
		return nil, ErrInvalidKey
	case len(key) > maxKeySize:
		return nil, exceedsSize("Key", maxKeySize, key)
	case int64(len(val)) > db.opt.ValueLogFileSize:
		return nil, exceedsSize("Value", db.opt.ValueLogFileSize, val)
	}
	entry := &Entry{
		Key:   key,
		Value: val,
	}
	req, err := db.sendToWriteCh([]*Entry{entry})
	if err != nil {
		return nil, fmt.Errorf("failed to send entry to write channel: %v", err)
	}
	return req, nil
}

type TableBuilderSizeKeyType int

const (
	TableBuilderSizeKeyCompressedBlockNum TableBuilderSizeKeyType = iota
	TableBuilderSizeKeyCompressedEntryNum
	TableBuilderSizeKeyCompressedSize
	TableBuilderSizeKeyUncompressedSize
	TableBuilderSizeKeyEncodedBlockNum
	TableBuilderSizeKeyEncodedEntryNum
	TableBuilderSizeKeyEncodedSize
	TableBuilderSizeKeyUnEncodedSize
)

type TableBuilderSizeKey struct {
	Type      TableBuilderSizeKeyType
	FromLevel int
	ToLevel   int
}

func (db *DB) compressAndEncodeStat(opts table.Options, from, to int) table.Options {
	if opts.Compression != options.None {
		if opts.State == nil {
			opts.State = &table.State{}
		}
		var size interface{}
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyUncompressedSize,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.UnCompressedSize = size.(*atomic.Int64)
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyCompressedSize,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.CompressedSize = size.(*atomic.Int64)
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyCompressedBlockNum,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.CompressedBlockNum = size.(*atomic.Int64)
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyCompressedEntryNum,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.CompressedEntryNum = size.(*atomic.Int64)
	}
	if opts.EncoderPool != nil {
		if opts.State == nil {
			opts.State = &table.State{}
		}
		var size interface{}
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyUnEncodedSize,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.UnEncodedSize = size.(*atomic.Int64)
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyEncodedSize,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.EncodedSize = size.(*atomic.Int64)
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyEncodedBlockNum,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.EncodedBlockNum = size.(*atomic.Int64)
		size, _ = db.stat.TableBuilderSize.LoadOrStore(TableBuilderSizeKey{
			Type:      TableBuilderSizeKeyEncodedEntryNum,
			FromLevel: from,
			ToLevel:   to,
		}, &atomic.Int64{})
		opts.State.EncodedEntryNum = size.(*atomic.Int64)
	}
	return opts

}

type Statistics struct {
	TableBuilderSize *sync.Map
}

func (db *DB) CollectStats() (s *Statistics) {
	return db.stat
}

var ErrTSetInvalidTS = errors.New("Timestamp should be greater than 0")

// TSet is a time-series set, which leverages internal version to store the timestamp.
type TSet struct {
	db *DB
}

func NewTSet(db *DB) *TSet {
	db.opt.NumVersionsToKeep = math.MaxInt64
	tSet := &TSet{db: db}
	return tSet
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
	vs, errLC := s.db.lc.get(y.KeyWithTs(key, ts), y.ValueStruct{}, 0)
	if errLC != nil {
		return nil, fmt.Errorf("faliled to get val from leved files: %v", errLC)
	}
	if vs.Value == nil {
		return nil, nil
	}
	return vs.Value, nil
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
	defer func(it y.Iterator) {
		_ = it.Close()
	}(it)
	r := make([][]byte, 0)
	for it.Seek(y.KeyWithTs(prefix, math.MaxUint64)); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() {
		r = append(r, it.Value().Value)
	}
	return r
}
