package table

import (
	"bytes"

	"github.com/dgraph-io/badger/v3/y"
)

var (
	emptyVal            = y.ValueStruct{}
	_        y.Iterator = (*ReducedUniIterator)(nil)
)

type ReduceFunc func(left bytes.Buffer, right y.ValueStruct) bytes.Buffer

type ReducedUniIterator struct {
	delegated  y.Iterator
	reduceFunc ReduceFunc
	k          []byte
	v          y.ValueStruct
}

func (r *ReducedUniIterator) Rewind() {
	r.delegated.Rewind()
	r.reduce()
}

func (r *ReducedUniIterator) Seek(key []byte) {
	r.delegated.Seek(key)
	r.reduce()
}

func (r *ReducedUniIterator) Next() {
	r.reduce()
}

func (r *ReducedUniIterator) Key() []byte {
	return r.k
}

func (r *ReducedUniIterator) Value() y.ValueStruct {
	return r.v
}

func (r *ReducedUniIterator) Valid() bool {
	return r.k != nil
}

func (r *ReducedUniIterator) Close() error {
	return r.delegated.Close()
}

func (r *ReducedUniIterator) reduce() {
	r.v = emptyVal
	r.k = nil
	if !r.delegated.Valid() {
		return
	}
	var buf bytes.Buffer
	maxVersion := y.ParseTs(r.delegated.Key())
	k := y.ParseKey(r.delegated.Key())
	for r.k = r.delegated.Key(); r.delegated.Valid() && y.SameKey(r.k, r.delegated.Key()); r.delegated.Next() {
		v := r.delegated.Value()
		v.Version = y.ParseTs(r.delegated.Key())
		if v.Version > maxVersion {
			maxVersion = v.Version
		}
		buf = r.reduceFunc(buf, v)
	}
	r.k = y.KeyWithTs(k, maxVersion)
	buf = r.reduceFunc(buf, emptyVal)
	r.v = y.ValueStruct{
		Value: buf.Bytes(),
	}
}

func NewReducedUniIterator(delegated y.Iterator, reducer ReduceFunc) *ReducedUniIterator {
	return &ReducedUniIterator{
		delegated:  delegated,
		reduceFunc: reducer,
	}
}
