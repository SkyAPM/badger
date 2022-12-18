package banyandb

// SeriesEncoderPool allows putting and getting SeriesEncoder.
type SeriesEncoderPool interface {
	Get(metadata []byte, buffer Buffer) SeriesEncoder
	Put(encoder SeriesEncoder)
}

// SeriesEncoder encodes time series data point.
type SeriesEncoder interface {
	// Append a data point
	Append(ts uint64, value []byte)
	// IsFull returns whether the encoded data reached its capacity
	IsFull() bool
	// Reset the underlying buffer
	Reset(key []byte, buffer Buffer)
	// Encode the time series data point to a binary
	Encode() error
	// StartTime indicates the first entry's time
	StartTime() uint64
}

// SeriesDecoderPool allows putting and getting SeriesDecoder.
type SeriesDecoderPool interface {
	Get(metadata []byte) SeriesDecoder
	Put(encoder SeriesDecoder)
}

// SeriesDecoder decodes encoded time series data.
type SeriesDecoder interface {
	// Decode the time series data
	Decode(key, data []byte) error
	// Len denotes the size of iterator
	Len() int
	// IsFull returns whether the encoded data reached its capacity
	IsFull() bool
	// Get the data point by its time
	Get(ts uint64) ([]byte, error)
	// Iterator returns a SeriesIterator
	Iterator() SeriesIterator
	// Range returns the start and end time of this series
	Range() (start, end uint64)
}

// SeriesIterator iterates time series data.
type SeriesIterator interface {
	// Next scroll the cursor to the next
	Next() bool
	// Val returns the value of the current data point
	Val() []byte
	// Time returns the time of the current data point
	Time() uint64
	// Error might return an error indicates a decode failure
	Error() error
}

type Buffer interface {
	Write(data []byte) (n int, err error)
	WriteByte(b byte) error
	Bytes() []byte
}
