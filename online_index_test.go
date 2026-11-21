package buntdb

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// testJSONDocs inserts n JSON documents of the form
// {"a": <n-i>, "b": <i>} with keys "doc:000001" etc, and returns the keys.
func testJSONDocs(t testing.TB, db *DB, n int) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("doc:%06d", i)
			val := fmt.Sprintf(`{"a":%d,"b":%d}`, n-i, i)
			if _, _, err := tx.Set(key, val, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// testIndexKeys returns all keys of an index in index order.
func testIndexKeys(t testing.TB, db *DB, index string) []string {
	t.Helper()
	var keys []string
	err := db.View(func(tx *Tx) error {
		return tx.Ascend(index, func(key, value string) bool {
			keys = append(keys, key)
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// testLiveKeys returns all keys of the database in key order.
func testLiveKeys(t testing.TB, db *DB) []string {
	t.Helper()
	var keys []string
	err := db.View(func(tx *Tx) error {
		return tx.Ascend("", func(key, value string) bool {
			keys = append(keys, key)
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func testSameKeys(t testing.TB, a, b []string) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("expecting %d keys, got %d", len(a), len(b))
	}
	set := make(map[string]bool, len(a))
	for _, k := range a {
		set[k] = true
	}
	for _, k := range b {
		if !set[k] {
			t.Fatalf("unexpected key '%v'", k)
		}
	}
}

// TestOnlineIndexRebuild tests the basic flow: an index on JSON field "a"
// is rebuilt online into an index on field "b" while the old index keeps
// serving, and the switch is visible to new transactions immediately.
func TestOnlineIndexRebuild(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 100)

	// create the initial index on field "a" through the online API so the
	// definition is comparable later.
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	// ordered by "a" descending relative to key order: doc:000099 first
	// because it has a=1.
	keys := testIndexKeys(t, db, "idx")
	if len(keys) != 100 || keys[0] != "doc:000099" {
		t.Fatalf("unexpected index order: %v", keys[:3])
	}

	// rebuild the same definition: rejected, no silent no-op.
	if _, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "a")); err != ErrIndexExists {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexExists, err)
	}

	// rebuild with a changed expression on the same name.
	rb, err = db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	// the old index keeps serving while the candidate exists.
	keys = testIndexKeys(t, db, "idx")
	if keys[0] != "doc:000099" {
		t.Fatalf("old index not serving during rebuild")
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	// new transactions immediately use the new definition: ordered by "b".
	keys = testIndexKeys(t, db, "idx")
	if len(keys) != 100 || keys[0] != "doc:000000" {
		t.Fatalf("new definition not in effect: %v", keys[:3])
	}
	// the new definition is now the serving one.
	if _, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b")); err != ErrIndexExists {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexExists, err)
	}
}

// TestOnlineIndexRebuildReplay tests that changes committed after the
// snapshot build are replayed into the candidate, so the switched-in index
// contains exactly the live keys.
func TestOnlineIndexRebuildReplay(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 100)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "doc:*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	// commit online writes while the rebuild is registered:
	// - overwrite some existing keys
	// - add new keys
	// - delete some keys
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("doc:%06d", i)
			if _, _, err := tx.Set(key, `{"a":0,"b":1000}`, nil); err != nil {
				return err
			}
		}
		for i := 100; i < 120; i++ {
			key := fmt.Sprintf("doc:%06d", i)
			val := fmt.Sprintf(`{"a":%d,"b":%d}`, i, i)
			if _, _, err := tx.Set(key, val, nil); err != nil {
				return err
			}
		}
		for i := 20; i < 30; i++ {
			if _, err := tx.Delete(fmt.Sprintf("doc:%06d", i)); err != nil {
				return err
			}
		}
		// a key that does not match the pattern must not enter the index
		if _, _, err := tx.Set("other:1", `{"b":1}`, nil); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	// the switched-in index contains exactly the live matching keys.
	var indexed []string
	err = db.View(func(tx *Tx) error {
		return tx.Ascend("idx", func(key, value string) bool {
			indexed = append(indexed, key)
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(indexed) != 110 {
		t.Fatalf("expecting 110 indexed keys, got %d", len(indexed))
	}
	for _, key := range indexed {
		if key == "other:1" {
			t.Fatal("non-matching key entered the index")
		}
		if key >= "doc:000020" && key <= "doc:000029" {
			t.Fatalf("deleted key '%v' still indexed", key)
		}
	}
	// overwritten values are ordered by their new value: the ten keys with
	// b=1000 come last.
	last := indexed[len(indexed)-10:]
	for _, key := range last {
		if key >= "doc:000010" {
			t.Fatalf("unexpected key '%v' among the b=1000 group", key)
		}
	}
}

// TestOnlineIndexRebuildConcurrentWrites runs a writer goroutine while a
// rebuild is building and switching, then verifies that the final index
// matches the live dataset exactly.
func TestOnlineIndexRebuildConcurrentWrites(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 5000
	testJSONDocs(t, db, n)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.Update(func(tx *Tx) error {
				key := fmt.Sprintf("doc:%06d", rand.Intn(n))
				val := fmt.Sprintf(`{"a":%d,"b":%d}`, i, i)
				if _, _, err := tx.Set(key, val, nil); err != nil {
					return err
				}
				i++
				return nil
			})
		}
	}()

	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "doc:*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	// every indexed key is live and every live matching key is indexed.
	indexed := testIndexKeys(t, db, "idx")
	live := testLiveKeys(t, db)
	testSameKeys(t, live, indexed)
	// the index order matches the live values of field "b".
	err = db.View(func(tx *Tx) error {
		var prev int = -1
		return tx.Ascend("idx", func(key, value string) bool {
			var b int
			fmt.Sscanf(value, `{"a":%*d,"b":%d}`, &b)
			if b < prev {
				t.Fatalf("index out of order at '%v'", key)
			}
			prev = b
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOnlineIndexRebuildRollback tests that rolled-back and failed writes
// never pollute the candidate structure.
func TestOnlineIndexRebuildRollback(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 50)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	// a write transaction that fails at the end must be rolled back and
	// must not reach the candidate.
	err = db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("doc:bad", `{"a":1,"b":1}`, nil); err != nil {
			return err
		}
		if _, err := tx.Delete("doc:000005"); err != nil {
			return err
		}
		return fmt.Errorf("forced failure")
	})
	if err == nil {
		t.Fatal("expecting an error")
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	indexed := testIndexKeys(t, db, "idx")
	if len(indexed) != 50 {
		t.Fatalf("expecting 50 indexed keys, got %d", len(indexed))
	}
	for _, key := range indexed {
		if key == "doc:bad" {
			t.Fatal("rolled-back key polluted the candidate")
		}
	}
}

// TestOnlineIndexRebuildAbort tests that aborting a rebuild keeps the old
// index serving and closes the handle.
func TestOnlineIndexRebuildAbort(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 50)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}

	rb, err = db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	// a second rebuild for the same name is rejected while one is active.
	if _, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b")); err != ErrIndexRebuildInProcess {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexRebuildInProcess, err)
	}
	if err := rb.Abort(); err != nil {
		t.Fatal(err)
	}
	// the handle is closed after abort.
	if err := rb.Abort(); err != ErrIndexRebuildClosed {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexRebuildClosed, err)
	}
	if err := rb.Commit(); err != ErrIndexRebuildClosed {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexRebuildClosed, err)
	}
	// the old index kept serving with the old definition.
	keys := testIndexKeys(t, db, "idx")
	if len(keys) != 50 || keys[0] != "doc:000049" {
		t.Fatalf("old index not serving after abort")
	}
	// writes after the abort go to the old index only.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("doc:new", `{"a":1,"b":1}`, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	keys = testIndexKeys(t, db, "idx")
	if len(keys) != 51 {
		t.Fatalf("expecting 51 indexed keys, got %d", len(keys))
	}
	if len(db.rebuilds) != 0 {
		t.Fatal("rebuild not unregistered after abort")
	}
}

// TestOnlineIndexRebuildCommitOnce tests that a rebuild handle can only be
// committed once.
func TestOnlineIndexRebuildCommitOnce(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 10)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != ErrIndexRebuildClosed {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexRebuildClosed, err)
	}
	if err := rb.Abort(); err != ErrIndexRebuildClosed {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexRebuildClosed, err)
	}
	if len(db.rebuilds) != 0 {
		t.Fatal("rebuild not unregistered after commit")
	}
}

// TestOnlineIndexRebuildDropConcurrent tests that dropping the index while
// a rebuild is building makes the rebuild's commit fail deterministically
// and releases the candidate.
func TestOnlineIndexRebuildDropConcurrent(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 50)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}

	rb, err = db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	// drop the index while the rebuild is registered.
	if err := db.DropIndex("idx"); err != nil {
		t.Fatal(err)
	}
	// the switch must fail: the definition it was built against is gone.
	if err := rb.Commit(); err != ErrIndexDefChanged {
		t.Fatalf("expecting '%v', got '%v'", ErrIndexDefChanged, err)
	}
	// deterministic state: the index stays dropped and no new index was
	// silently created.
	if _, err := db.Indexes(); err != nil {
		t.Fatal(err)
	}
	err = db.View(func(tx *Tx) error {
		return tx.Ascend("idx", func(key, value string) bool { return true })
	})
	if err != ErrNotFound {
		t.Fatalf("expecting '%v', got '%v'", ErrNotFound, err)
	}
	// resources are released.
	if len(db.rebuilds) != 0 {
		t.Fatal("rebuild not unregistered after failed commit")
	}
	// the database keeps accepting writes.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("doc:new", `{"a":1}`, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestOnlineIndexRebuildDeleteAll tests that a DeleteAll committed while a
// rebuild is registered resets the candidate, and the switched-in index
// contains only the keys that are live after the reset.
func TestOnlineIndexRebuildDeleteAll(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 100)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}

	rb, err = db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	// reset the dataset mid-rebuild, then write fresh keys.
	if err := db.Update(func(tx *Tx) error {
		if err := tx.DeleteAll(); err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("fresh:%d", i)
			val := fmt.Sprintf(`{"a":%d,"b":%d}`, i, i)
			if _, _, err := tx.Set(key, val, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	indexed := testIndexKeys(t, db, "idx")
	if len(indexed) != 10 {
		t.Fatalf("expecting 10 indexed keys, got %d", len(indexed))
	}
	for i, key := range indexed {
		if key != fmt.Sprintf("fresh:%d", i) {
			t.Fatalf("unexpected key '%v' at position %d", key, i)
		}
	}
}

// TestOnlineIndexRebuildReaderSnapshot tests that a read transaction that
// started before the switch keeps using the old index snapshot, and that
// the switch commits exactly once the old readers are done.
func TestOnlineIndexRebuildReaderSnapshot(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testJSONDocs(t, db, 50)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}

	rb, err = db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}

	// open a read transaction before the switch.
	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	committed := make(chan error, 1)
	go func() {
		committed <- rb.Commit()
	}()
	// the commit blocks until the read transaction finishes.
	select {
	case <-committed:
		t.Fatal("commit finished while a read transaction was open")
	case <-time.After(time.Millisecond * 100):
	}
	// the old read transaction still uses the old definition (field "a").
	var first string
	if err := rtx.Ascend("idx", func(key, value string) bool {
		first = key
		return false
	}); err != nil {
		t.Fatal(err)
	}
	if first != "doc:000049" {
		t.Fatalf("old reader not on old snapshot, got '%v'", first)
	}
	if err := rtx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	// new transactions use the new definition (field "b").
	keys := testIndexKeys(t, db, "idx")
	if keys[0] != "doc:000000" {
		t.Fatalf("new definition not in effect, got '%v'", keys[0])
	}
}

// TestOnlineIndexRebuildSpatial tests online rebuilding of a spatial
// index, including replay of online writes.
func TestOnlineIndexRebuildSpatial(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("pt:%d", i)
			if _, _, err := tx.Set(key, Point(float64(i), float64(i)), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := NewSpatialIndexSpec("pts", "pt:*", "rect", IndexRect)
	rb, err := db.BeginIndexRebuild(spec)
	if err != nil {
		t.Fatal(err)
	}
	// online writes while the rebuild is registered.
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("pt:100", Point(100, 100), nil); err != nil {
			return err
		}
		_, err := tx.Delete("pt:0")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	err = db.View(func(tx *Tx) error {
		return tx.Intersects("pts", Rect([]float64{0, 0}, []float64{200, 200}),
			func(key, value string) bool {
				count++
				return true
			})
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expecting 100 points, got %d", count)
	}
}

// TestOnlineIndexRebuildPersisted tests that the replay hook also works
// for databases that persist to disk.
func TestOnlineIndexRebuildPersisted(t *testing.T) {
	db := testOpen(t)
	defer testClose(db)

	testJSONDocs(t, db, 100)
	rb, err := db.BeginIndexRebuild(JSONIndexSpec("idx", "*", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 100; i < 150; i++ {
			key := fmt.Sprintf("doc:%06d", i)
			val := fmt.Sprintf(`{"a":%d,"b":%d}`, i, i)
			if _, _, err := tx.Set(key, val, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := rb.Commit(); err != nil {
		t.Fatal(err)
	}
	indexed := testIndexKeys(t, db, "idx")
	if len(indexed) != 150 {
		t.Fatalf("expecting 150 indexed keys, got %d", len(indexed))
	}
}
