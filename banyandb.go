package badger

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
)

//HandoverIterator MUST run serially.
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

type Statistics struct {
	MemBytes int64
}

func (db *DB) Stats() (s Statistics) {
	tt, relFn := db.getMemTables()
	defer relFn()
	for _, mt := range tt {
		// Stats Memtable size
		s.MemBytes += mt.sl.MemSize()
	}
	return s
}
