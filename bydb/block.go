package bydb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v3/y"
)

var _ TSetEncoder = (*BlockEncoder)(nil)
var _ TSetDecoder = (*BlockDecoder)(nil)

//BlockEncoder backport to reduced value
type BlockEncoder struct {
	tsBuff    bytes.Buffer
	valBuff   bytes.Buffer
	scratch   [binary.MaxVarintLen64]byte
	len       uint32
	num       uint32
	startTime uint64
	valueSize int
}

func NewBlockEncoder(size int) TSetEncoder {
	return &BlockEncoder{
		valueSize: size,
	}
}

func (t *BlockEncoder) Append(ts uint64, value []byte) {
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

func (t *BlockEncoder) IsFull() bool {
	return t.valBuff.Len() >= t.valueSize
}

func (t *BlockEncoder) Reset(_ []byte) {
	t.tsBuff.Reset()
	t.valBuff.Reset()
	t.num = 0
	t.startTime = 0
}

func (t *BlockEncoder) Encode() ([]byte, error) {
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

func (t *BlockEncoder) StartTime() uint64 {
	return t.startTime
}

func (t *BlockEncoder) putUint16(v uint16) []byte {
	binary.LittleEndian.PutUint16(t.scratch[:], v)
	return t.scratch[:2]
}

func (t *BlockEncoder) putUint32(v uint32) []byte {
	binary.LittleEndian.PutUint32(t.scratch[:], v)
	return t.scratch[:4]
}

func (t *BlockEncoder) putUint64(v uint64) []byte {
	binary.LittleEndian.PutUint64(t.scratch[:], v)
	return t.scratch[:8]
}

const (
	// TsLen equals ts(uint64) + data_offset(uint32)
	TsLen = 8 + 4
)

var ErrInvalidValue = errors.New("invalid encoded value")

//BlockDecoder decodes encoded time index
type BlockDecoder struct {
	ts        []byte
	val       []byte
	len       uint32
	num       uint32
	valueSize int
}

func NewBlockDecoder(size int) TSetDecoder {
	return &BlockDecoder{
		valueSize: size,
	}
}

func (t *BlockDecoder) Len() int {
	return int(t.num)
}

func (t *BlockDecoder) Decode(_, rawData []byte) (err error) {
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

func (t *BlockDecoder) IsFull() bool {
	return int(t.len) >= t.valueSize
}

func (t *BlockDecoder) Get(ts uint64) ([]byte, error) {
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

func (t *BlockDecoder) Iterator() TSetIterator {
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

func newBlockItemIterator(decoder *BlockDecoder) TSetIterator {
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
