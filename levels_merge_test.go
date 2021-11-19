package badger

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v3/bydb"
)

var encoderPool = bydb.NewBlockEncoderPool(math.MaxInt64)

func TestCompactionMergeAllVersions(t *testing.T) {
	// Disable compactions and keep all versions of the each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(math.MaxInt32).WithExternalCompactor(
		encoderPool,
		bydb.NewBlockDecoderPool(math.MaxInt64),
	)
	opt.managedTxns = true
	t.Run("with overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", encodeValue("bar3"), 3, bydb.BitCompact}, {"fooz", "baz", 1, 0}}
			l2 := []keyValVersion{{"foo", encodeValue("bar2"), 2, bydb.BitCompact}}
			l3 := []keyValVersion{{"foo", encodeValue("bar1"), 1, bydb.BitCompact}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)
			createAndOpen(db, l3, 3)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", encodeValue("bar3"), 3, bydb.BitCompact},
				{"foo", encodeValue("bar2"), 2, bydb.BitCompact},
				{"foo", encodeValue("bar1"), 1, bydb.BitCompact},
				{"fooz", "baz", 1, 0},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[1],
				nextLevel: db.lc.levels[2],
				top:       db.lc.levels[1].tables,
				bot:       db.lc.levels[2].tables,
				t:         db.lc.levelTargets(),
			}
			cdef.t.baseLevel = 2
			require.NoError(t, db.lc.runCompactDef(-1, 1, cdef))
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", encodeValue("bar3", "bar2"), 2, bydb.BitCompact},
				{"foo", encodeValue("bar1"), 1, bydb.BitCompact},
				{"fooz", "baz", 1, 0},
			})

			cdef = compactDef{
				thisLevel: db.lc.levels[2],
				nextLevel: db.lc.levels[3],
				top:       db.lc.levels[2].tables,
				bot:       db.lc.levels[3].tables,
				t:         db.lc.levelTargets(),
			}
			cdef.t.baseLevel = 3
			require.NoError(t, db.lc.runCompactDef(-1, 2, cdef))
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", encodeValue("bar3", "bar2", "bar1"), 1, bydb.BitCompact},
				{"fooz", "baz", 1, 0},
			})
		})
	})
	t.Run("without overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", encodeValue("bar3"), 3, bydb.BitCompact}, {"fooz", encodeValue("baz1"), 1, bydb.BitCompact}}
			l2 := []keyValVersion{{"fooo", encodeValue("barr2"), 2, bydb.BitCompact}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", encodeValue("bar3"), 3, bydb.BitCompact}, {"fooo", encodeValue("barr2"), 2, bydb.BitCompact}, {"fooz", encodeValue("baz1"), 1, bydb.BitCompact},
			})
			cdef := compactDef{
				thisLevel: db.lc.levels[1],
				nextLevel: db.lc.levels[2],
				top:       db.lc.levels[1].tables,
				bot:       db.lc.levels[2].tables,
				t:         db.lc.levelTargets(),
			}
			cdef.t.baseLevel = 2
			require.NoError(t, db.lc.runCompactDef(-1, 1, cdef))
			getAllAndCheck(t, db, []keyValVersion{
				{"foo", encodeValue("bar3"), 3, bydb.BitCompact}, {"fooo", encodeValue("barr2"), 2, bydb.BitCompact}, {"fooz", encodeValue("baz1"), 1, bydb.BitCompact},
			})
		})
	})
}

func encodeValue(values ...string) string {
	encoder := encoderPool.Get(nil)
	defer encoderPool.Put(encoder)
	for _, each := range values {
		v, err := strconv.Atoi(each[len(each)-1:])
		if err != nil {
			panic(err)
		}
		encoder.Append(uint64(v), []byte(each))
	}
	str, err := encoder.Encode()
	if err != nil {
		panic(err)
	}
	return string(str)
}

//TODO: add more cases
