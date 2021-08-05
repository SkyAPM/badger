package badger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactionMergeAllVersions(t *testing.T) {
	// Disable compactions and keep all versions of the each key.
	opt := DefaultOptions("").WithNumCompactors(0).WithNumVersionsToKeep(math.MaxInt32)
	opt.managedTxns = true
	opt.MergeFunc = func(existingVal, newVal []byte) []byte {
		if existingVal == nil {
			return newVal
		}
		return append(existingVal, newVal...)
	}
	t.Run("with overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", "bar3", 3, bitMergeEntry}, {"fooz", "baz", 1, 0}}
			l2 := []keyValVersion{{"foo", "bar2", 2, bitMergeEntry}}
			l3 := []keyValVersion{{"foo", "bar1", 1, bitMergeEntry}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)
			createAndOpen(db, l3, 3)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar3", 3, bitMergeEntry},
				{"foo", "bar2", 2, bitMergeEntry},
				{"foo", "bar1", 1, bitMergeEntry},
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
				{"foo", "bar3bar2", 3, bitMergeEntry},
				{"foo", "bar1", 1, bitMergeEntry},
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
				{"foo", "bar3bar2bar1", 3, bitMergeEntry},
				{"fooz", "baz", 1, 0},
			})
		})
	})
	t.Run("without overlap", func(t *testing.T) {
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			l1 := []keyValVersion{{"foo", "bar", 3, bitMergeEntry}, {"fooz", "baz", 1, bitMergeEntry}}
			l2 := []keyValVersion{{"fooo", "barr", 2, bitMergeEntry}}
			createAndOpen(db, l1, 1)
			createAndOpen(db, l2, 2)

			// Set a high discard timestamp so that all the keys are below the discard timestamp.
			db.SetDiscardTs(10)

			getAllAndCheck(t, db, []keyValVersion{
				{"foo", "bar", 3, bitMergeEntry}, {"fooo", "barr", 2, bitMergeEntry}, {"fooz", "baz", 1, bitMergeEntry},
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
				{"foo", "bar", 3, bitMergeEntry}, {"fooo", "barr", 2, bitMergeEntry}, {"fooz", "baz", 1, bitMergeEntry},
			})
		})
	})
}
