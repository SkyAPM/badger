package bydb

import "github.com/pkg/errors"

const BitCompact byte = 1 << 5

var ErrEncodeEmpty = errors.New("encode an empty value")

// TSetEncoder encodes time series data point
type TSetEncoder interface {
	// Append a data point
	Append(ts uint64, value []byte)
	// IsFull returns whether the encoded data reached its capacity
	IsFull() bool
	// Reset the underlying buffer
	Reset(key []byte)
	// Encode the time series data point to a binary
	Encode() ([]byte, error)
	// StartTime indicates the first entry's time
	StartTime() uint64
}

type TSetEncoderFactory func() TSetEncoder

// TSetDecoder decodes encoded time series data
type TSetDecoder interface {
	// Decode the time series data
	Decode(key, data []byte) error
	// Len denotes the size of iterator
	Len() int
	// IsFull returns whether the encoded data reached its capacity
	IsFull() bool
	// Get the data point by its time
	Get(ts uint64) ([]byte, error)
	// Iterator returns a TSetIterator
	Iterator() TSetIterator
}

type TSetDecoderFactory func() TSetDecoder

// TSetIterator iterates time series data
type TSetIterator interface {
	// Next scroll the cursor to the next
	Next() bool
	// Val returns the value of the current data point
	Val() []byte
	// Time returns the time of the current data point
	Time() uint64
	// Error might return an error indicates a decode failure
	Error() error
}
