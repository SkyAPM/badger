package bydb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v3/y"
)

var (
	encoderPool = sync.Pool{
		New: func() interface{} {
			return &blockEncoder{}
		},
	}
	decoderPool = sync.Pool{
		New: func() interface{} {
			return &blockDecoder{}
		},
	}
)

var (
	_ TSetEncoder     = (*blockEncoder)(nil)
	_ TSetDecoder     = (*blockDecoder)(nil)
	_ TSetEncoderPool = (*blockEncoderPool)(nil)
)

type blockEncoderPool struct {
	pool *sync.Pool
	size int
}

func NewBlockEncoderPool(size int) TSetEncoderPool {
	return &blockEncoderPool{
		pool: &encoderPool,
		size: size,
	}
}

func (b *blockEncoderPool) Get(metadata []byte) TSetEncoder {
	encoder := b.pool.Get().(*blockEncoder)
	encoder.Reset(metadata)
	encoder.valueSize = b.size
	return encoder
}

func (b *blockEncoderPool) Put(encoder TSetEncoder) {
	b.pool.Put(encoder)
}

type blockDecoderPool struct {
	pool *sync.Pool
	size int
}

func NewBlockDecoderPool(size int) TSetDecoderPool {
	return &blockDecoderPool{
		pool: &decoderPool,
		size: size,
	}
}

func (b *blockDecoderPool) Get(_ []byte) TSetDecoder {
	decoder := b.pool.Get().(*blockDecoder)
	decoder.valueSize = b.size
	return decoder
}

func (b *blockDecoderPool) Put(decoder TSetDecoder) {
	b.pool.Put(decoder)
}

//blockEncoder backport to reduced value
type blockEncoder struct {
	tsBuff    bytes.Buffer
	valBuff   bytes.Buffer
	scratch   [binary.MaxVarintLen64]byte
	len       uint32
	num       uint32
	startTime uint64
	valueSize int
}

func (t *blockEncoder) Append(ts uint64, value []byte) {
	if t.startTime == 0 {
		t.startTime = ts
	} else if t.startTime > ts {
		t.startTime = ts
	}
	vLen := len(value)
	offset := uint32(len(t.valBuff.Bytes()))
	t.valBuff.Write(t.putUint32(uint32(vLen)))
	t.valBuff.Write(value)
	t.tsBuff.Write(t.putUint64(ts))
	t.tsBuff.Write(t.putUint32(offset))
	t.num = t.num + 1
}

func (t *blockEncoder) IsFull() bool {
	return t.valBuff.Len() >= t.valueSize
}

func (t *blockEncoder) Reset(_ []byte) {
	t.tsBuff.Reset()
	t.valBuff.Reset()
	t.num = 0
	t.startTime = 0
}

func (t *blockEncoder) Encode() ([]byte, error) {
	if t.tsBuff.Len() < 1 {
		return nil, ErrEncodeEmpty
	}
	val := t.valBuff.Bytes()
	t.len = uint32(len(val))
	_, err := t.tsBuff.WriteTo(&t.valBuff)
	if err != nil {
		return nil, err
	}
	t.valBuff.Write(t.putUint32(t.num))
	t.valBuff.Write(t.putUint32(t.len))
	data := t.valBuff.Bytes()
	l := len(data)
	dst := make([]byte, 0, y.ZSTDCompressBound(l))
	if dst, err = y.ZSTDCompress(dst, data, 1); err != nil {
		return nil, err
	}
	result := make([]byte, len(dst)+2)
	copy(result, dst)
	copy(result[len(dst):], t.putUint16(uint16(l)))
	return result, nil
}

func (t *blockEncoder) StartTime() uint64 {
	return t.startTime
}

func (t *blockEncoder) putUint16(v uint16) []byte {
	binary.LittleEndian.PutUint16(t.scratch[:], v)
	return t.scratch[:2]
}

func (t *blockEncoder) putUint32(v uint32) []byte {
	binary.LittleEndian.PutUint32(t.scratch[:], v)
	return t.scratch[:4]
}

func (t *blockEncoder) putUint64(v uint64) []byte {
	binary.LittleEndian.PutUint64(t.scratch[:], v)
	return t.scratch[:8]
}

const (
	// TsLen equals ts(uint64) + data_offset(uint32)
	TsLen = 8 + 4
)

var ErrInvalidValue = errors.New("invalid encoded value")

//blockDecoder decodes encoded time index
type blockDecoder struct {
	ts        []byte
	val       []byte
	len       uint32
	num       uint32
	valueSize int
}

func (t *blockDecoder) Len() int {
	return int(t.num)
}

func (t *blockDecoder) Decode(_, rawData []byte) (err error) {
	var data []byte
	size := binary.LittleEndian.Uint16(rawData[len(rawData)-2:])
	if data, err = y.ZSTDDecompress(make([]byte, 0, size), rawData[:len(rawData)-2]); err != nil {
		return err
	}
	l := uint32(len(data))
	if l <= 8 {
		return ErrInvalidValue
	}
	lenOffset := len(data) - 4
	numOffset := lenOffset - 4
	t.num = binary.LittleEndian.Uint32(data[numOffset:lenOffset])
	t.len = binary.LittleEndian.Uint32(data[lenOffset:])
	if l <= t.len+8 {
		return ErrInvalidValue
	}
	t.val = data[:t.len]
	t.ts = data[t.len:numOffset]
	return nil
}

func (t *blockDecoder) IsFull() bool {
	return int(t.len) >= t.valueSize
}

func (t *blockDecoder) Get(ts uint64) ([]byte, error) {
	i := sort.Search(int(t.num), func(i int) bool {
		slot := getTSSlot(t.ts, i)
		return parseTS(slot) <= ts
	})
	if i >= int(t.num) {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	slot := getTSSlot(t.ts, i)
	if parseTS(slot) != ts {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	return getVal(t.val, parseOffset(slot))
}

func (t *blockDecoder) Iterator() TSetIterator {
	return newBlockItemIterator(t)
}

func getVal(buf []byte, offset uint32) ([]byte, error) {
	if uint32(len(buf)) <= offset+4 {
		return nil, ErrInvalidValue
	}
	dataLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
	return buf[offset+4 : offset+4+dataLen], nil
}

func getTSSlot(data []byte, index int) []byte {
	return data[index*TsLen : (index+1)*TsLen]
}

func parseTS(tsSlot []byte) uint64 {
	return binary.LittleEndian.Uint64(tsSlot[:8])
}

func parseOffset(tsSlot []byte) uint32 {
	return binary.LittleEndian.Uint32(tsSlot[8:])
}

var _ TSetIterator = (*blockItemIterator)(nil)

type blockItemIterator struct {
	index []byte
	data  []byte
	idx   int
	num   int
}

func newBlockItemIterator(decoder *blockDecoder) TSetIterator {
	return &blockItemIterator{
		idx:   -1,
		index: decoder.ts,
		data:  decoder.val,
		num:   int(decoder.num),
	}
}

func (b *blockItemIterator) Next() bool {
	b.idx++
	return b.idx >= 0 && b.idx < b.num
}

func (b *blockItemIterator) Val() []byte {
	v, _ := getVal(b.data, parseOffset(getTSSlot(b.index, b.idx)))
	return v
}

func (b *blockItemIterator) Time() uint64 {
	return parseTS(getTSSlot(b.index, b.idx))
}

func (b *blockItemIterator) Error() error {
	return nil
}
