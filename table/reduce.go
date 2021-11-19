package table

import (
	"github.com/dgraph-io/badger/v3/bydb"
	"github.com/dgraph-io/badger/v3/y"
)

var (
	emptyVal            = y.ValueStruct{}
	_        y.Iterator = (*ReducedUniIterator)(nil)
)

type ReducedUniIterator struct {
	delegated    y.Iterator
	k            []byte
	v            y.ValueStruct
	metricEnable bool
	encoderPool  bydb.TSetEncoderPool
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
	k := y.ParseKey(r.delegated.Key())
	encoder := r.encoderPool.Get(k)
	defer r.encoderPool.Put(encoder)
	for r.k = r.delegated.Key(); r.delegated.Valid() && y.SameKey(r.k, r.delegated.Key()); r.delegated.Next() {
		v := r.delegated.Value()
		y.NumTSetFanOutEntities(r.metricEnable, 1)
		encoder.Append(y.ParseTs(r.delegated.Key()), v.Value)
		if encoder.IsFull() {
			r.delegated.Next()
			break
		}
	}
	r.k = y.KeyWithTs(k, encoder.StartTime())
	val, _ := encoder.Encode()
	meta := bydb.BitCompact
	if encoder.IsFull() {
		meta = 0
	}
	r.v = y.ValueStruct{
		Value: val,
		Meta:  meta,
	}
	y.NumTSetFanOut(r.metricEnable, 1)
}

type ReducedUniIteratorOptions func(iterator *ReducedUniIterator)

func WithMetricEnable(metricEnable bool) ReducedUniIteratorOptions {
	return func(iterator *ReducedUniIterator) {
		iterator.metricEnable = metricEnable
	}
}

func WithEncoderPool(encoderPool bydb.TSetEncoderPool) ReducedUniIteratorOptions {
	return func(iterator *ReducedUniIterator) {
		iterator.encoderPool = encoderPool
	}
}

func NewReducedUniIterator(delegated y.Iterator, opt ...ReducedUniIteratorOptions) *ReducedUniIterator {
	r := &ReducedUniIterator{
		delegated: delegated,
	}
	for _, option := range opt {
		option(r)
	}
	return r
}
