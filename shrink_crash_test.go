package buntdb

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testOpenTemp opens a persisted database inside a temporary directory
// that is removed when the test finishes.
func testOpenTemp(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return db, path
}

// testFill writes count keys named key0..keyN with values val0..valN.
func testFill(t *testing.T, db *DB, count int) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("key%d", i)
			if _, _, err := tx.Set(key, fmt.Sprintf("val%d", i), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// testCheckAll verifies that count keys exist with their expected values.
func testCheckAll(t *testing.T, db *DB, count int) {
	t.Helper()
	err := db.View(func(tx *Tx) error {
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if n != count {
			t.Fatalf("expected %d items, got %d", count, n)
		}
		for i := 0; i < count; i++ {
			val, err := tx.Get(fmt.Sprintf("key%d", i))
			if err != nil {
				return err
			}
			if val != fmt.Sprintf("val%d", i) {
				t.Fatalf("expected val%d, got %s", i, val)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestShrinkCrashSafetyBasic verifies that a shrunk database keeps all of
// its data, that the footer is removed after a successful shrink, and that
// the data survives a reopen.
func TestShrinkCrashSafetyBasic(t *testing.T) {
	db, path := testOpenTemp(t)
	testFill(t, db, 1000)
	// create redundant log entries
	testFill(t, db, 1000)
	fi1, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Size() >= fi1.Size() {
		t.Fatalf("expected shrunk file to be smaller: %d >= %d",
			fi2.Size(), fi1.Size())
	}
	// after a successful shrink the footer must have been truncated away.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if hasShrinkFooter(data[len(data)-int(shrinkFooterSize):]) {
		t.Fatal("footer should have been truncated after a successful shrink")
	}
	testCheckAll(t, db, 1000)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// reopen and verify everything survived.
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	testCheckAll(t, db, 1000)
}

// TestShrinkCrashBeforeReplace simulates a crash that happened while the
// tmp file was being written: the old aof must remain authoritative and
// the stale tmp file must be removed on open.
func TestShrinkCrashBeforeReplace(t *testing.T) {
	db, path := testOpenTemp(t)
	testFill(t, db, 100)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// simulate a crashed shrink: a half-written tmp file next to the aof.
	tmpname := path + ".tmp"
	if err := os.WriteFile(tmpname, []byte("*3\r\n$3\r\nset\r\n$3\r\nkey"), 0666); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := os.Stat(tmpname); !os.IsNotExist(err) {
		t.Fatal("stale tmp file should have been removed on open")
	}
	testCheckAll(t, db, 100)
}

// writeFooterToFile appends a valid shrink footer to the file at path and
// returns the payload length.
func writeFooterToFile(t *testing.T, path string) int64 {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	footer := appendShrinkFooter(nil, int64(len(payload)),
		crc32.ChecksumIEEE(payload))
	if len(footer) != int(shrinkFooterSize) {
		t.Fatalf("footer has wrong size: %d", len(footer))
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(footer); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return int64(len(payload))
}

// TestShrinkCrashAfterReplace simulates a crash that happened after the
// atomic rename but before the footer was truncated: the new aof must must
// be recovered through its footer, and only the new log (payload plus
// replay segment) is loaded.
func TestShrinkCrashAfterReplace(t *testing.T) {
	db, path := testOpenTemp(t)
	testFill(t, db, 100)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// simulate the post-rename pre-truncate crash state.
	payloadLen := writeFooterToFile(t, path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	testCheckAll(t, db, 100)
	// the footer must have been truncated during the load.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != payloadLen {
		t.Fatalf("expected file size %d after footer truncation, got %d",
			payloadLen, fi.Size())
	}
	// appends after the recovery must not corrupt the file, even commands
	// that are shorter than the footer they overwrite.
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Delete("key0"); err != nil {
			return err
		}
		_, _, err := tx.Set("a", "b", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.View(func(tx *Tx) error {
		// key0 was deleted and "a" was added: 100 items in total.
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if n != 100 {
			return fmt.Errorf("expected 100 items, got %d", n)
		}
		for i := 1; i < 100; i++ {
			val, err := tx.Get(fmt.Sprintf("key%d", i))
			if err != nil {
				return err
			}
			if val != fmt.Sprintf("val%d", i) {
				return fmt.Errorf("expected val%d, got %s", i, val)
			}
		}
		if _, err := tx.Get("key0"); err != ErrNotFound {
			return fmt.Errorf("expected key0 to be deleted")
		}
		val, err := tx.Get("a")
		if err != nil {
			return err
		}
		if val != "b" {
			return fmt.Errorf("expected b, got %s", val)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestShrinkCorruptFooterRejected verifies that a file carrying a shrink
// tail marker is rejected when the length or the checksum does not match.
func TestShrinkCorruptFooterRejected(t *testing.T) {
	build := func(t *testing.T) string {
		db, path := testOpenTemp(t)
		testFill(t, db, 100)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		writeFooterToFile(t, path)
		return path
	}
	expectInvalid := func(t *testing.T, path string) {
		t.Helper()
		db, err := Open(path)
		if err == nil {
			_ = db.Close()
			t.Fatal("expected open to fail with ErrInvalid")
		}
		if err != ErrInvalid {
			t.Fatalf("expected ErrInvalid, got %v", err)
		}
	}
	t.Run("CorruptPayload", func(t *testing.T) {
		path := build(t)
		f, err := os.OpenFile(path, os.O_WRONLY, 0666)
		if err != nil {
			t.Fatal(err)
		}
		// flip a byte in the middle of the payload
		if _, err := f.WriteAt([]byte{0xFF}, 50); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		expectInvalid(t, path)
	})
	t.Run("CorruptChecksum", func(t *testing.T) {
		path := build(t)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// corrupt the crc field of the footer
		data[len(data)-10] = '0'
		data[len(data)-9] = '0'
		data[len(data)-8] = '0'
		data[len(data)-7] = '0'
		data[len(data)-6] = '0'
		data[len(data)-5] = '0'
		data[len(data)-4] = '0'
		data[len(data)-3] = '0'
		if err := os.WriteFile(path, data, 0666); err != nil {
			t.Fatal(err)
		}
		expectInvalid(t, path)
	})
	t.Run("CorruptLength", func(t *testing.T) {
		path := build(t)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// truncate the payload by one byte so the length no longer matches
		if err := os.WriteFile(path, data[:len(data)-1], 0666); err != nil {
			t.Fatal(err)
		}
		expectInvalid(t, path)
	})
}

// TestShrinkConcurrentWrites hammers the database with writes while a
// shrink is in progress. Every committed write must survive the switch:
// writes before the snapshot are in the rewritten data, writes during the
// switch are in the replay segment.
func TestShrinkConcurrentWrites(t *testing.T) {
	db, path := testOpenTemp(t)
	testFill(t, db, 2000)
	const writers = 4
	const perWriter = 250
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d-%d", w, i)
				_ = db.Update(func(tx *Tx) error {
					_, _, err := tx.Set(key, fmt.Sprintf("value-%d", i), nil)
					return err
				})
			}
		}(w)
	}
	// shrink while the writers are running
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	// shrink again, now that all writes are done, and verify.
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	check := func(db *DB) {
		t.Helper()
		err := db.View(func(tx *Tx) error {
			// 2000 initial keys plus the keys from the concurrent writers.
			n, err := tx.Len()
			if err != nil {
				return err
			}
			if n != 2000+writers*perWriter {
				return fmt.Errorf("expected %d items, got %d",
					2000+writers*perWriter, n)
			}
			for i := 0; i < 2000; i++ {
				val, err := tx.Get(fmt.Sprintf("key%d", i))
				if err != nil {
					return err
				}
				if val != fmt.Sprintf("val%d", i) {
					return fmt.Errorf("bad value for key%d: %s", i, val)
				}
			}
			for w := 0; w < writers; w++ {
				for i := 0; i < perWriter; i++ {
					val, err := tx.Get(fmt.Sprintf("w%d-%d", w, i))
					if err != nil {
						return err
					}
					if val != fmt.Sprintf("value-%d", i) {
						return fmt.Errorf("bad value for w%d-%d: %s", w, i, val)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	check(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// everything must survive a reopen as well.
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	check(db)
}

// TestShrinkCloseRace runs Shrink and Close concurrently. Exactly one
// outcome is allowed: either the shrink completes and Close closes the new
// file, or Close wins and the shrink aborts with ErrDatabaseClosed. A
// repeated Close must return ErrDatabaseClosed and must not delete the
// new file.
func TestShrinkCloseRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		db, path := testOpenTemp(t)
		testFill(t, db, 100)
		var wg sync.WaitGroup
		var shrinkErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			shrinkErr = db.Shrink()
		}()
		go func() {
			defer wg.Done()
			_ = db.Close()
		}()
		wg.Wait()
		if shrinkErr != nil && shrinkErr != ErrDatabaseClosed {
			t.Fatalf("unexpected shrink error: %v", shrinkErr)
		}
		// repeated Close must report ErrDatabaseClosed...
		if err := db.Close(); err != ErrDatabaseClosed {
			t.Fatalf("expected ErrDatabaseClosed, got %v", err)
		}
		// ...and must not have deleted the data file.
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("data file missing after close race: %v", err)
		}
		// the file, old or new, must be fully recoverable.
		db, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		testCheckAll(t, db, 100)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestShrinkRetry verifies that a shrink error is retryable: a concurrent
// shrink fails with ErrShrinkInProcess and a subsequent shrink succeeds.
func TestShrinkRetry(t *testing.T) {
	db, _ := testOpenTemp(t)
	defer func() { _ = db.Close() }()
	testFill(t, db, 5000)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = db.Shrink()
		}(i)
	}
	wg.Wait()
	if errs[0] != ErrShrinkInProcess && errs[1] != ErrShrinkInProcess {
		t.Fatal("expected one shrink to report ErrShrinkInProcess")
	}
	// the failed call is retryable.
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	testCheckAll(t, db, 5000)
}

// TestShrinkIndexAndTTL verifies that indexes and TTLs are consistent
// after a shrink and a replay of the new log.
func TestShrinkIndexAndTTL(t *testing.T) {
	db, path := testOpenTemp(t)
	if err := db.CreateIndex("names", "*", IndexString); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if _, _, err := tx.Set(fmt.Sprintf("user%d", i),
				fmt.Sprintf("name%03d", i), nil); err != nil {
				return err
			}
		}
		_, _, err := tx.Set("ttlkey", "ttlval",
			&SetOptions{Expires: true, TTL: time.Hour})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	// the index must still be fully functional after the shrink.
	if err := db.View(func(tx *Tx) error {
		var count int
		if err := tx.Ascend("names", func(key, value string) bool {
			count++
			return true
		}); err != nil {
			return err
		}
		if count != 101 {
			return fmt.Errorf("expected 101 items through index, got %d", count)
		}
		ttl, err := tx.TTL("ttlkey")
		if err != nil {
			return err
		}
		if ttl <= 0 || ttl > time.Hour {
			return fmt.Errorf("bad ttl after shrink: %v", ttl)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// after a reopen the TTL must be preserved (absolute expiration).
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.View(func(tx *Tx) error {
		ttl, err := tx.TTL("ttlkey")
		if err != nil {
			return err
		}
		if ttl <= 0 || ttl > time.Hour {
			return fmt.Errorf("bad ttl after reopen: %v", ttl)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestShrinkNoDuplicateExpireEvents verifies that replaying the shrunk log
// does not re-trigger expiration events for historical keys: a key whose
// expiration is already recorded in the log must be dropped silently on
// load.
func TestShrinkNoDuplicateExpireEvents(t *testing.T) {
	db, path := testOpenTemp(t)
	// set a key that will expire while the database is closed.
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("exp1", "val",
			&SetOptions{Expires: true, TTL: time.Second}); err != nil {
			return err
		}
		_, _, err := tx.Set("keep1", "val", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// shrink while the key is still alive so that its expiration command
	// lands in the shrunk file.
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// let the key expire while the database is closed.
	time.Sleep(time.Second * 2)
	// reopen with an expiration handler. The historical expiration must
	// not fire again.
	fired := make(chan string, 10)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.SetConfig(Config{
		SyncPolicy: EverySecond,
		OnExpired: func(keys []string) {
			for _, k := range keys {
				fired <- k
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	// the expired key must be gone and must not produce an event.
	select {
	case key := <-fired:
		t.Fatalf("historical expiration event fired for key %q", key)
	case <-time.After(time.Millisecond * 2500):
	}
	if err := db.View(func(tx *Tx) error {
		if _, err := tx.Get("exp1"); err != ErrNotFound {
			return fmt.Errorf("expected exp1 to be gone")
		}
		if _, err := tx.Get("keep1"); err != nil {
			return fmt.Errorf("expected keep1 to survive: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestShrinkAppendOnlyPreserved verifies the normal append-only behavior
// after a shrink: new commands are appended behind the payload, partial
// commands at the tail are truncated on load, and small appends (shorter
// than the removed footer) do not corrupt the file.
func TestShrinkAppendOnlyPreserved(t *testing.T) {
	db, path := testOpenTemp(t)
	testFill(t, db, 100)
	if err := db.Shrink(); err != nil {
		t.Fatal(err)
	}
	// append small commands after the shrink.
	if err := db.Update(func(tx *Tx) error {
		_, _, err := tx.Set("a", "b", nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.Delete("key5")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// simulate a torn write at the tail of the file.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("*3\r\n$3\r\nset\r\n$5\r\npar")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.View(func(tx *Tx) error {
		// key5 was deleted and "a" was added: 100 items in total.
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if n != 100 {
			return fmt.Errorf("expected 100 items, got %d", n)
		}
		if _, err := tx.Get("key5"); err != ErrNotFound {
			return fmt.Errorf("expected key5 to be deleted")
		}
		val, err := tx.Get("a")
		if err != nil {
			return err
		}
		if val != "b" {
			return fmt.Errorf("expected b, got %s", val)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
