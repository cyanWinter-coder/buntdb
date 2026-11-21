package buntdb

import (
	"strings"

	"github.com/tidwall/rtred"
)

// This file implements online index rebuilding. It allows an index
// definition (for example a JSON path expression) to be changed while the
// database keeps serving reads and writes:
//
//   1. BeginIndexRebuild builds the new index in a candidate structure from
//      a consistent snapshot of the live keys. The old index keeps serving
//      all transactions while the candidate is building, and keeps serving
//      if the build is aborted or fails.
//   2. Changes committed by write transactions while the rebuild is
//      registered are replayed into the candidate, so the switched-in index
//      contains every live key at the moment of the switch.
//   3. Index definitions are comparable through IndexSpec. A rebuild for a
//      name whose existing definition is identical is rejected with
//      ErrIndexExists; a same-named index with a different expression is
//      fully rebuilt and atomically replaced, never silently mixing entries
//      from the two definitions.
//   4. IndexRebuild.Commit performs the switch exactly once, under the
//      database write lock. Read transactions that started before the
//      switch keep using the previous index snapshot.
//   5. Transactions that begin after the switch immediately use the new
//      definition. Rolled-back or failed writes never reach the candidate.
//   6. Dropping or replacing the index while a rebuild is building makes
//      the rebuild's Commit fail deterministically with ErrIndexDefChanged
//      and releases the candidate. DeleteAll resets in-process rebuilds.

// IndexSpec is a comparable description of an index definition. It is used
// with BeginIndexRebuild to build and switch in a new index online.
//
// The Expression field is a plain string that uniquely identifies the
// ordering expression of the index (for example "json:age" for an index
// built with IndexJSON("age")). Two definitions with the same Pattern,
// Expression, Options, and index kind (ordered vs spatial) are considered
// identical, regardless of the actual less/rect function values, which are
// not comparable in Go.
type IndexSpec struct {
	// Name is the name of the index.
	Name string
	// Pattern is the key pattern, same as CreateIndex.
	Pattern string
	// Expression uniquely identifies the ordering expression. It is only
	// used for comparing definitions; it does not affect ordering.
	Expression string
	// Options are the index options, same as CreateIndexOptions.
	Options IndexOptions

	less func(a, b string) bool                 // ordering function
	rect func(item string) (min, max []float64) // spatial function
}

// NewIndexSpec returns an IndexSpec for an ordered (b-tree) index. The
// expression should uniquely identify the less ordering, for example
// "json:age" or "int". It is used to detect definition changes.
func NewIndexSpec(name, pattern, expression string,
	less ...func(a, b string) bool) IndexSpec {
	return IndexSpec{
		Name:       name,
		Pattern:    pattern,
		Expression: expression,
		less:       makeLessFunc(less),
	}
}

// NewSpatialIndexSpec returns an IndexSpec for a spatial (r-tree) index.
// The expression should uniquely identify the rect function.
func NewSpatialIndexSpec(name, pattern, expression string,
	rect func(item string) (min, max []float64)) IndexSpec {
	return IndexSpec{
		Name:       name,
		Pattern:    pattern,
		Expression: expression,
		rect:       rect,
	}
}

// JSONIndexSpec returns an IndexSpec for an index on a JSON field, ordered
// by IndexJSON(path). The expression is "json:"+path, so two specs for the
// same path compare as identical and specs for different paths do not.
func JSONIndexSpec(name, pattern, path string) IndexSpec {
	return NewIndexSpec(name, pattern, "json:"+path, IndexJSON(path))
}

// indexDef is the comparable portion of an index definition.
type indexDef struct {
	pattern    string
	expression string
	spatial    bool
	opts       IndexOptions
}

// defOf returns the comparable definition of an index, or nil when the
// index does not exist.
func defOf(idx *index) *indexDef {
	if idx == nil {
		return nil
	}
	return &indexDef{
		pattern:    idx.pattern,
		expression: idx.expression,
		spatial:    idx.rect != nil,
		opts:       idx.opts,
	}
}

// equalDefs reports whether two definitions are identical. A nil definition
// represents a missing index.
func equalDefs(a, b *indexDef) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// buildIndex creates an empty index from the spec.
func (spec IndexSpec) buildIndex(db *DB) *index {
	pattern := spec.Pattern
	if spec.Options.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	idx := &index{
		name:       spec.Name,
		pattern:    pattern,
		expression: spec.Expression,
		less:       spec.less,
		rect:       spec.rect,
		db:         db,
		opts:       spec.Options,
	}
	if idx.less != nil {
		idx.btr = btreeNew(lessCtx(idx))
	}
	if idx.rect != nil {
		idx.rtr = rtred.New(idx)
	}
	return idx
}

// IndexRebuild is a handle to an in-process online index rebuild, created
// by BeginIndexRebuild. The candidate index is kept current with all
// committed writes until Commit atomically switches it in, or Abort
// discards it. A handle is single-use: after Commit or Abort it is closed.
type IndexRebuild struct {
	db       *DB                // the origin database
	spec     IndexSpec          // the definition being built
	cand     *index             // the candidate index
	candKeys map[string]*dbItem // items currently in the candidate, by key
	base     *indexDef          // live definition when the rebuild began
	gen      uint64             // dataset generation at last (re)start
}

// BeginIndexRebuild starts an online rebuild of the index described by
// spec. The new index is built in a candidate structure from a consistent
// snapshot of the live keys, in chunks so that reads and writes continue
// while the build is running. Changes committed after the rebuild is
// registered are replayed into the candidate.
//
// If an index with the same name and an identical definition already
// exists, ErrIndexExists is returned. If a rebuild for the same name is
// already in-process, ErrIndexRebuildInProcess is returned.
//
// The returned handle must be closed with Commit (to atomically switch the
// new index in) or Abort (to discard the candidate and keep the old index
// serving).
func (db *DB) BeginIndexRebuild(spec IndexSpec) (*IndexRebuild, error) {
	if spec.Name == "" {
		// an empty name is designated for the main "keys" tree.
		return nil, ErrInvalidOperation
	}
	if spec.less == nil && spec.rect == nil {
		return nil, ErrInvalidOperation
	}
	rb := &IndexRebuild{db: db, spec: spec}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrDatabaseClosed
	}
	for _, other := range db.rebuilds {
		if other.spec.Name == spec.Name {
			db.mu.Unlock()
			return nil, ErrIndexRebuildInProcess
		}
	}
	rb.resetLocked()
	// Reject a rebuild that would produce a definition identical to the
	// one already serving. Definitions that differ (for example a changed
	// JSON expression) are rebuilt from scratch and atomically replaced,
	// never merged with the existing entries.
	if equalDefs(defOf(db.idxs[spec.Name]), defOf(rb.cand)) {
		db.mu.Unlock()
		return nil, ErrIndexExists
	}
	rb.base = defOf(db.idxs[spec.Name])
	rb.gen = db.idxGen
	// Register the rebuild before releasing the lock so that every write
	// transaction that commits from this point on replays its changes
	// into the candidate.
	db.rebuilds = append(db.rebuilds, rb)
	db.mu.Unlock()

	// Scan the live keys in chunks, releasing the lock between chunks so
	// that other readers and writers are not blocked for the duration of
	// the build. Committed changes that occur between chunks are applied
	// to the candidate by the commit replay hook; scanInsertLocked never
	// overwrites a replayed (newer or equal) version of an item.
	pivot := ""
	for {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			_ = rb.Abort()
			return nil, ErrDatabaseClosed
		}
		if rb.gen != db.idxGen {
			// The dataset was reset (DeleteAll) since the last chunk.
			// The commit hook already reset the candidate; restart the
			// scan against the new keys tree.
			rb.gen = db.idxGen
			pivot = ""
		}
		done := true
		n := 0
		btreeAscendGreaterOrEqual(db.keys, &dbItem{key: pivot},
			func(item interface{}) bool {
				dbi := item.(*dbItem)
				if n >= 1000 {
					pivot = dbi.key
					done = false
					return false
				}
				rb.scanInsertLocked(dbi)
				n++
				return true
			},
		)
		db.mu.RUnlock()
		if done {
			break
		}
	}
	return rb, nil
}

// Commit atomically switches the candidate index in. The switch happens
// once, under the database write lock: read transactions that started
// before the switch keep using the previous index snapshot, and
// transactions that start after the switch immediately use the new
// definition.
//
// If the live index definition changed since BeginIndexRebuild (for
// example the index was dropped or replaced by another transaction),
// ErrIndexDefChanged is returned and the candidate is discarded. If the
// handle was already committed or aborted, ErrIndexRebuildClosed is
// returned.
func (rb *IndexRebuild) Commit() error {
	if rb.db == nil {
		return ErrIndexRebuildClosed
	}
	db := rb.db
	db.mu.Lock()
	defer db.mu.Unlock()
	rb.removeLocked()
	rb.db = nil
	if db.closed {
		rb.cand, rb.candKeys = nil, nil
		return ErrDatabaseClosed
	}
	// Optimistic check: the live definition must be the same as when the
	// rebuild began. Otherwise the candidate (built against the old
	// definition's lifecycle) cannot be safely switched in.
	if !equalDefs(defOf(db.idxs[rb.spec.Name]), rb.base) {
		rb.cand, rb.candKeys = nil, nil
		return ErrIndexDefChanged
	}
	// Atomically replace the index. The previous index object is left
	// untouched for any snapshot readers and is released to the garbage
	// collector.
	db.idxs[rb.spec.Name] = rb.cand
	rb.cand, rb.candKeys = nil, nil
	return nil
}

// Abort discards the candidate index and closes the rebuild handle. The
// previously serving index, if any, is unaffected.
func (rb *IndexRebuild) Abort() error {
	if rb.db == nil {
		return ErrIndexRebuildClosed
	}
	db := rb.db
	db.mu.Lock()
	rb.removeLocked()
	db.mu.Unlock()
	rb.db = nil
	rb.cand, rb.candKeys = nil, nil
	return nil
}

// removeLocked unregisters the rebuild from the database.
// The database write lock must be held.
func (rb *IndexRebuild) removeLocked() {
	for i, other := range rb.db.rebuilds {
		if other == rb {
			rb.db.rebuilds = append(rb.db.rebuilds[:i],
				rb.db.rebuilds[i+1:]...)
			return
		}
	}
}

// resetLocked resets the candidate to an empty state. Called when the
// rebuild begins and when the dataset is reset (DeleteAll) while the
// rebuild is registered.
// The database lock must be held.
func (rb *IndexRebuild) resetLocked() {
	rb.cand = rb.spec.buildIndex(rb.db)
	rb.candKeys = make(map[string]*dbItem)
}

// scanInsertLocked inserts an item encountered by the snapshot scan into
// the candidate. Items already present in the candidate were placed there
// by the commit replay hook with a version that is at least as new as
// anything the scan can observe, so they are never overwritten.
// The database lock must be held.
func (rb *IndexRebuild) scanInsertLocked(dbi *dbItem) {
	if _, ok := rb.candKeys[dbi.key]; ok {
		return
	}
	if !rb.cand.match(dbi.key) {
		return
	}
	rb.candKeys[dbi.key] = dbi
	if rb.cand.btr != nil {
		rb.cand.btr.Set(dbi)
	}
	if rb.cand.rtr != nil {
		rb.cand.rtr.Insert(dbi)
	}
}

// applyLocked replays a single committed change into the candidate. A nil
// item means the key was deleted.
// The database write lock must be held (called from Tx.Commit).
func (rb *IndexRebuild) applyLocked(key string, item *dbItem) {
	if old, ok := rb.candKeys[key]; ok {
		if rb.cand.btr != nil {
			rb.cand.btr.Delete(old)
		}
		if rb.cand.rtr != nil {
			rb.cand.rtr.Remove(old)
		}
		delete(rb.candKeys, key)
	}
	if item == nil {
		return
	}
	if !rb.cand.match(item.key) {
		return
	}
	rb.candKeys[item.key] = item
	if rb.cand.btr != nil {
		rb.cand.btr.Set(item)
	}
	if rb.cand.rtr != nil {
		rb.cand.rtr.Insert(item)
	}
}
