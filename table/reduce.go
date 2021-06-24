package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/pkg/errors"
)

const (
	// TsLen equals ts(uint64) + data_offset(uint32)
	TsLen = 8 + 4
)

var (
	emptyVal                   = y.ValueStruct{}
	_               y.Iterator = (*ReducedUniIterator)(nil)
	_               y.Iterator = (*reducedValueIterator)(nil)
	ErrInvalidValue            = errors.New("invalid reduced value")
)

type ReducedUniIterator struct {
	delegated     y.Iterator
	k             []byte
	v             y.ValueStruct
	compressLevel int
	valueSize     int
	metricEnable  bool
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
	reducedValue := NewReducedValue(r.compressLevel, r.valueSize)
	reducedValue.metricEnable = r.metricEnable
	minVersion := y.ParseTs(r.delegated.Key())
	k := y.ParseKey(r.delegated.Key())
	for r.k = r.delegated.Key(); r.delegated.Valid() && y.SameKey(r.k, r.delegated.Key()); r.delegated.Next() {
		v := r.delegated.Value()
		v.Version = y.ParseTs(r.delegated.Key())
		if !reducedValue.Append(v) {
			break
		}
		if v.Version < minVersion {
			minVersion = v.Version
		}
		y.NumTSetFanOutEntities(r.metricEnable, 1)
	}
	r.k = y.KeyWithTs(k, minVersion)
	val, _ := reducedValue.Marshal()
	r.v = y.ValueStruct{
		Value: val,
	}
	y.NumTSetFanOut(r.metricEnable, 1)
}

type ReducedUniIteratorOptions func(iterator *ReducedUniIterator)

func WithMetricEnable(metricEnable bool) ReducedUniIteratorOptions {
	return func(iterator *ReducedUniIterator) {
		iterator.metricEnable = metricEnable
	}
}

func NewReducedUniIterator(delegated y.Iterator, compressLevel, valueSize int, opt ...ReducedUniIteratorOptions) *ReducedUniIterator {
	r := &ReducedUniIterator{
		delegated:     delegated,
		compressLevel: compressLevel,
		valueSize:     valueSize,
	}
	for _, option := range opt {
		option(r)
	}
	return r
}

func Uint16ToBytes(u uint16) []byte {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, u)
	return bs
}

func BytesToUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

func Uint32ToBytes(u uint32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, u)
	return bs
}

func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func Uint64ToBytes(u uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, u)
	return bs
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

//ReducedValue TODO: implement ROF compression
type ReducedValue struct {
	tsBuff        bytes.Buffer
	valBuff       bytes.Buffer
	ts            []byte
	val           []byte
	len           uint32
	num           uint32
	compressLevel int
	valueSize     int
	metricEnable  bool
}

func NewReducedValue(compressLevel, valueSize int) *ReducedValue {
	if compressLevel == 0 {
		compressLevel = 3
	}
	if valueSize == 0 {
		valueSize = math.MaxInt64
	}
	return &ReducedValue{
		compressLevel: compressLevel,
		valueSize:     valueSize,
	}
}

func (r *ReducedValue) Append(val y.ValueStruct) bool {
	v := val.Value
	vLen := len(v)
	if r.valBuff.Len()+vLen > r.valueSize {
		return false
	}
	offset := uint32(len(r.valBuff.Bytes()))
	r.valBuff.Write(Uint32ToBytes(uint32(vLen)))
	r.valBuff.Write(v)
	r.tsBuff.Write(Uint64ToBytes(val.Version))
	r.tsBuff.Write(Uint32ToBytes(offset))
	r.num = r.num + 1
	return true
}

func (r *ReducedValue) Marshal() (data []byte, err error) {
	val := r.valBuff.Bytes()
	r.len = uint32(len(val))
	data = make([]byte, 0, 8+r.tsBuff.Len()+len(val))
	data = append(data, Uint32ToBytes(r.num)...)
	data = append(data, Uint32ToBytes(r.len)...)
	data = append(data, val...)
	data = append(data, r.tsBuff.Bytes()...)
	l := len(data)
	y.NumTSetFanOutSize(r.metricEnable, int64(l))
	if r.compressLevel > -1 {
		dst := make([]byte, 0, y.ZSTDCompressBound(l))
		if dst, err = y.ZSTDCompress(dst, data, r.compressLevel); err != nil {
			return nil, err
		}
		y.NumTSetFanOutCompressedSize(r.metricEnable, int64(len(dst)))
		result := make([]byte, 0, len(dst)+2)
		result = append(result, Uint16ToBytes(uint16(l))...)
		result = append(result, dst...)
		return result, nil
	}
	return data, err
}

func (r *ReducedValue) Unmarshal(rawData []byte) (err error) {
	var data []byte
	if r.compressLevel > -1 {
		size := BytesToUint16(rawData[:2])
		if data, err = y.ZSTDDecompress(make([]byte, 0, size), rawData[2:]); err != nil {
			return err
		}
	} else {
		data = rawData
	}
	l := uint32(len(data))
	if l <= 8 {
		return ErrInvalidValue
	}
	r.num = BytesToUint32(data[:4])
	r.len = BytesToUint32(data[4:8])
	if l <= r.len+8 {
		return ErrInvalidValue
	}
	r.val = data[8 : r.len+8]
	r.ts = data[r.len+8:]
	return nil
}

func (r *ReducedValue) Get(ts uint64) ([]byte, error) {
	i := sort.Search(int(r.num), func(i int) bool {
		slot := getTSSlot(r.ts, i)
		return BytesToUint64(parseTS(slot)) <= ts
	})
	if i >= int(r.num) {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	slot := getTSSlot(r.ts, i)
	if BytesToUint64(parseTS(slot)) != ts {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	return getVal(r.val, parseOffset(slot))
}

func getVal(buf []byte, offset uint32) ([]byte, error) {
	if uint32(len(buf)) <= offset+4 {
		return nil, ErrInvalidValue
	}
	dataLen := BytesToUint32(buf[offset : offset+4])
	return buf[offset+4 : offset+4+dataLen], nil
}

func getTSSlot(data []byte, index int) []byte {
	return data[index*TsLen : (index+1)*TsLen]
}

func parseTS(tsSlot []byte) []byte {
	return tsSlot[:8]
}

func parseOffset(tsSlot []byte) uint32 {
	return BytesToUint32(tsSlot[8:])
}

func (r *ReducedValue) Iter(reversed bool) y.Iterator {
	return &reducedValueIterator{
		index:    r.ts,
		data:     r.val,
		reversed: reversed,
		num:      int(r.num),
	}
}

func (r *ReducedValue) Len() uint32 {
	return r.len
}

type reducedValueIterator struct {
	index    []byte
	data     []byte
	idx      int
	reversed bool
	num      int
}

func (r *reducedValueIterator) Next() {
	if !r.reversed {
		r.idx++
	} else {
		r.idx--
	}
}

func (r *reducedValueIterator) Rewind() {
	if !r.reversed {
		r.idx = 0
	} else {
		r.idx = r.num - 1
	}
}

func (r *reducedValueIterator) Seek(key []byte) {
	if !r.reversed {
		r.idx = sort.Search(r.num, func(i int) bool {
			return bytes.Compare(parseTS(getTSSlot(r.index, i)), key) <= 0
		})
	} else {
		r.idx = r.num - 1 - sort.Search(r.num, func(i int) bool {
			return bytes.Compare(parseTS(getTSSlot(r.index, r.num-1-i)), key) >= 0
		})
	}
}

func (r *reducedValueIterator) Key() []byte {
	return parseTS(getTSSlot(r.index, r.idx))
}

func (r *reducedValueIterator) Value() y.ValueStruct {
	v, _ := getVal(r.data, parseOffset(getTSSlot(r.index, r.idx)))
	return y.ValueStruct{
		Value: v,
	}
}

func (r *reducedValueIterator) Valid() bool {
	return r.idx >= 0 && r.idx < r.num
}

func (r *reducedValueIterator) Close() error {
	return nil
}
