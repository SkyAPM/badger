module github.com/dgraph-io/badger/v3

go 1.12

// replace github.com/dgraph-io/ristretto => /home/amanbansal/go/src/github.com/dgraph-io/ristretto

require (
	github.com/cespare/xxhash v1.1.0
	github.com/dgraph-io/ristretto v0.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/google/flatbuffers v1.12.1
	github.com/klauspost/compress v1.12.3
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.5
	golang.org/x/net v0.0.0-20210525063256-abc453219eb5
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9
)
