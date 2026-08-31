package litestream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/superfly/ltx"
)

func TestActivity160599SameSizeWALRecycleProducesNextLTX(t *testing.T) {
	const (
		pageSize = 4096
		oldSalt1 = 0x11111111
		oldSalt2 = 0x22222222
		newSalt1 = 0x33333333
		newSalt2 = 0x44444444
	)

	dir := t.TempDir()
	db := NewDB(filepath.Join(dir, "source.db"))
	oldPage := bytes.Repeat([]byte{0xA1}, pageSize)
	newPage := bytes.Repeat([]byte{0xB2}, pageSize)
	if err := os.WriteFile(db.Path(), oldPage, 0o600); err != nil {
		t.Fatal(err)
	}
	db.pageSize = pageSize
	db.fileInfo, _ = os.Stat(db.Path())
	db.dirInfo, _ = os.Stat(dir)
	var err error
	if db.f, err = os.Open(db.Path()); err != nil {
		t.Fatal(err)
	}
	defer db.f.Close()

	ltxData := activity160599BuildLTX(t, 1, 1, pageSize, 1, oldPage, oldSalt1, oldSalt2, WALHeaderSize, WALFrameHeaderSize+pageSize)
	ltxPath := db.LTXPath(0, 1, 1)
	if err := os.MkdirAll(filepath.Dir(ltxPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ltxPath, ltxData, 0o600); err != nil {
		t.Fatal(err)
	}

	wal := activity160599BuildWAL(t, pageSize, newSalt1, newSalt2, newPage)
	if err := os.WriteFile(db.WALPath(), wal, 0o600); err != nil {
		t.Fatal(err)
	}

	info, err := db.verify(context.Background())
	if err != nil {
		t.Fatalf("verify recycled WAL: %v", err)
	}
	if !info.snapshotting {
		t.Fatalf("same-size WAL with new salts was classified as idle: offset=%d reason=%q", info.offset, info.reason)
	}
	if info.reason == "" {
		t.Fatal("recycled WAL must have a diagnostic reason")
	}

	synced, err := db.sync(context.Background(), false, info)
	if err != nil {
		t.Fatalf("sync recycled WAL: %v", err)
	}
	if !synced {
		t.Fatal("same-size recycled WAL did not create a new LTX transaction")
	}
	if pos, err := db.Pos(); err != nil {
		t.Fatal(err)
	} else if pos.TXID != 2 {
		t.Fatalf("position after recycled WAL sync=%s, want 2", pos.TXID)
	}

	f, err := os.Open(db.LTXPath(0, 2, 2))
	if err != nil {
		t.Fatalf("open next LTX: %v", err)
	}
	defer f.Close()
	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("decode next LTX header: %v", err)
	}
	var phdr ltx.PageHeader
	gotPage := make([]byte, pageSize)
	if err := dec.DecodePage(&phdr, gotPage); err != nil {
		t.Fatalf("decode next LTX page: %v", err)
	}
	if phdr.Pgno != 1 || !bytes.Equal(gotPage, newPage) {
		t.Fatalf("next LTX does not contain recycled WAL commit: pgno=%d first_byte=%#x", phdr.Pgno, gotPage[0])
	}
	if err := dec.DecodePage(&phdr, gotPage); !errors.Is(err, io.EOF) {
		t.Fatalf("next LTX has unexpected additional page: %v", err)
	}
	if err := dec.Close(); err != nil {
		t.Fatalf("validate next LTX trailer: %v", err)
	}
}

func TestActivity160599RestoreRefusesExistingDestination(t *testing.T) {
	output := filepath.Join(t.TempDir(), "existing.db")
	want := []byte("existing database must remain untouched")
	if err := os.WriteFile(output, want, 0o600); err != nil {
		t.Fatal(err)
	}

	err := NewReplicaWithClient(nil, &activity160599Client{}).Restore(
		context.Background(), RestoreOptions{OutputPath: output},
	)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("restore error=%v, want existing-destination refusal", err)
	}
	got, readErr := os.ReadFile(output)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("existing destination changed: got %q, want %q", got, want)
	}
}

func TestActivity160599RestoreTargetAndTimestampSemantics(t *testing.T) {
	snapshot := &ltx.FileInfo{
		Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1,
		Size: ltx.HeaderSize, CreatedAt: time.Unix(100, 0),
	}
	increment2 := &ltx.FileInfo{
		Level: 0, MinTXID: 2, MaxTXID: 2,
		Size: ltx.HeaderSize, CreatedAt: time.Unix(200, 0),
	}
	increment3 := &ltx.FileInfo{
		Level: 0, MinTXID: 3, MaxTXID: 3,
		Size: ltx.HeaderSize, CreatedAt: time.Unix(300, 0),
	}
	client := &activity160599Client{files: func(level int) []*ltx.FileInfo {
		switch level {
		case SnapshotLevel:
			return []*ltx.FileInfo{snapshot}
		case 0:
			return []*ltx.FileInfo{increment2, increment3}
		default:
			return nil
		}
	}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	assertTarget := func(name string, txid ltx.TXID, timestamp time.Time, want ltx.TXID) {
		t.Helper()
		plan, err := CalcRestorePlan(context.Background(), client, txid, timestamp, logger)
		if err != nil {
			t.Fatalf("%s: calc restore plan: %v", name, err)
		}
		if len(plan) == 0 || plan[len(plan)-1].MaxTXID != want {
			t.Fatalf("%s: plan=%v, want final txid %s", name, plan, want)
		}
	}

	assertTarget("explicit txid", 2, time.Time{}, 2)
	assertTarget("point in time", 0, time.Unix(250, 0), 2)
	assertTarget("timestamp boundary is exclusive", 0, time.Unix(200, 0), 1)
	if _, err := CalcRestorePlan(context.Background(), client, 2, time.Unix(250, 0), logger); err == nil {
		t.Fatal("expected simultaneous txid and timestamp targets to be rejected")
	}
}

func TestActivity160599LTXWireFormatRejectsSQLiteLockPage(t *testing.T) {
	const pageSize = 4096
	lockPgno := ltx.LockPgno(pageSize)
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum,
		PageSize: pageSize, Commit: lockPgno, MinTXID: 2, MaxTXID: 2,
	}); err != nil {
		t.Fatal(err)
	}
	err = enc.EncodePage(
		ltx.PageHeader{Pgno: lockPgno},
		bytes.Repeat([]byte{0xE7}, pageSize),
	)
	if err == nil || !strings.Contains(err.Error(), "lock page") {
		t.Fatalf("encode lock page error=%v, want explicit refusal", err)
	}
}

func TestActivity160599ContinuousRestoreNormalizesSQLiteHeader(t *testing.T) {
	const pageSize = 4096
	page1 := bytes.Repeat([]byte{0x71}, pageSize)
	page1[18], page1[19] = 0x02, 0x02
	binary.BigEndian.PutUint32(page1[24:28], 0x11223344)
	data := activity160599BuildLTX(
		t, 2, 2, pageSize, 1, page1, 7, 8, WALHeaderSize, WALFrameHeaderSize+pageSize,
	)
	info := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(data))}
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == 0 {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(data, offset, size)
		},
	}

	path := filepath.Join(t.TempDir(), "lock-page.db")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(pageSize); err != nil {
		t.Fatal(err)
	}

	if err := NewReplicaWithClient(nil, client).applyLTXFile(context.Background(), f, info, pageSize); err != nil {
		t.Fatalf("apply continuous restore update: %v", err)
	}
	header := make([]byte, 28)
	if _, err := f.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	if header[18] != 0x01 || header[19] != 0x01 {
		t.Fatalf("sqlite header journal bytes=%#x %#x, want DELETE mode", header[18], header[19])
	}
	if binary.BigEndian.Uint32(header[24:28]) == 0x11223344 {
		t.Fatal("sqlite schema change counter was not normalized")
	}
}

func TestActivity160599RestoreReplansDeletedFileWithoutMovingTarget(t *testing.T) {
	const pageSize = 4096
	snapshotData := activity160599BuildLTX(t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x41}, pageSize), 1, 2, WALHeaderSize, 0)
	incrementData := activity160599BuildLTX(t, 2, 2, pageSize, 1, bytes.Repeat([]byte{0x42}, pageSize), 3, 4, WALHeaderSize, WALFrameHeaderSize+pageSize)

	snapshot := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(snapshotData)), CreatedAt: time.Unix(100, 0)}
	deleted := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(incrementData)), CreatedAt: time.Unix(200, 0)}
	alternative := &ltx.FileInfo{Level: 1, MinTXID: 2, MaxTXID: 2, Size: int64(len(incrementData)), CreatedAt: time.Unix(200, 0)}
	newer := activity160599BuildLTX(t, 3, 3, pageSize, 1, bytes.Repeat([]byte{0x43}, pageSize), 5, 6, WALHeaderSize, WALFrameHeaderSize+pageSize)

	client := &activity160599Client{}
	client.files = func(level int) []*ltx.FileInfo {
		client.mu.Lock()
		defer client.mu.Unlock()
		if level == SnapshotLevel {
			return []*ltx.FileInfo{snapshot}
		}
		if !client.deleted {
			if level == 0 {
				return []*ltx.FileInfo{deleted}
			}
			return nil
		}
		if level == 1 {
			return []*ltx.FileInfo{alternative}
		}
		if level == 0 {
			// A later transaction appears after the first plan. A correct retry
			// remains pinned to transaction 2 instead of silently moving forward.
			return []*ltx.FileInfo{{Level: 0, MinTXID: 3, MaxTXID: 3, Size: int64(len(newer)), CreatedAt: time.Unix(300, 0)}}
		}
		return nil
	}
	client.open = func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
		key := activity160599Key(level, minTXID, maxTXID)
		client.mu.Lock()
		if key == activity160599Key(0, 2, 2) && !client.deleted {
			client.deleted = true
			client.mu.Unlock()
			return nil, fmt.Errorf("retention removed planned object: %w", os.ErrNotExist)
		}
		client.mu.Unlock()

		var data []byte
		switch key {
		case activity160599Key(SnapshotLevel, 1, 1):
			data = snapshotData
		case activity160599Key(1, 2, 2):
			data = incrementData
		case activity160599Key(0, 3, 3):
			data = newer
		default:
			return nil, os.ErrNotExist
		}
		return activity160599RangeReader(data, offset, size)
	}

	output := filepath.Join(t.TempDir(), "restored.db")
	r := NewReplicaWithClient(nil, client)
	if err := r.Restore(context.Background(), RestoreOptions{OutputPath: output}); err != nil {
		t.Fatalf("restore after retention race: %v", err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pageSize || got[0] != 0x42 {
		t.Fatalf("restored wrong target: size=%d first_byte=%#x, want txid 2 image", len(got), got[0])
	}
	if got[0] == 0x43 {
		t.Fatal("retry drifted to a newer transaction")
	}
}

func TestActivity160599RestoreReplansMidStreamDeletionWithoutMovingTarget(t *testing.T) {
	const pageSize = 4096
	snapshotData := activity160599BuildLTX(t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x31}, pageSize), 1, 2, WALHeaderSize, 0)
	incrementData := activity160599BuildLTX(t, 2, 2, pageSize, 1, bytes.Repeat([]byte{0x32}, pageSize), 3, 4, WALHeaderSize, WALFrameHeaderSize+pageSize)
	newerData := activity160599BuildLTX(t, 3, 3, pageSize, 1, bytes.Repeat([]byte{0x33}, pageSize), 5, 6, WALHeaderSize, WALFrameHeaderSize+pageSize)

	snapshot := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(snapshotData))}
	deleted := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(incrementData))}
	alternative := &ltx.FileInfo{Level: 1, MinTXID: 2, MaxTXID: 2, Size: int64(len(incrementData))}
	newer := &ltx.FileInfo{Level: 0, MinTXID: 3, MaxTXID: 3, Size: int64(len(newerData))}

	client := &activity160599Client{}
	client.files = func(level int) []*ltx.FileInfo {
		client.mu.Lock()
		defer client.mu.Unlock()
		if level == SnapshotLevel {
			return []*ltx.FileInfo{snapshot}
		}
		if !client.deleted && level == 0 {
			return []*ltx.FileInfo{deleted}
		}
		if client.deleted && level == 1 {
			return []*ltx.FileInfo{alternative}
		}
		if client.deleted && level == 0 {
			return []*ltx.FileInfo{newer}
		}
		return nil
	}
	client.open = func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
		switch activity160599Key(level, minTXID, maxTXID) {
		case activity160599Key(SnapshotLevel, 1, 1):
			return activity160599RangeReader(snapshotData, offset, size)
		case activity160599Key(0, 2, 2):
			client.mu.Lock()
			defer client.mu.Unlock()
			if offset == 0 && !client.deleted {
				cut := ltx.HeaderSize + 37
				return &activity160599FaultReader{data: incrementData, cut: cut, terminalErr: io.ErrUnexpectedEOF}, nil
			}
			client.deleted = true
			return nil, fmt.Errorf("range vanished during restore: %w", os.ErrNotExist)
		case activity160599Key(1, 2, 2):
			return activity160599RangeReader(incrementData, offset, size)
		case activity160599Key(0, 3, 3):
			return activity160599RangeReader(newerData, offset, size)
		default:
			return nil, os.ErrNotExist
		}
	}

	output := filepath.Join(t.TempDir(), "midstream-replan.db")
	if err := NewReplicaWithClient(nil, client).Restore(context.Background(), RestoreOptions{OutputPath: output}); err != nil {
		t.Fatalf("restore after mid-stream retention deletion: %v", err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pageSize || got[0] != 0x32 {
		t.Fatalf("restore moved off frozen target: size=%d first_byte=%#x", len(got), got[0])
	}
}

func TestActivity160599RestoreMissingPlanIsBoundedAndClean(t *testing.T) {
	const pageSize = 4096
	snapshotData := activity160599BuildLTX(t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x51}, pageSize), 1, 2, WALHeaderSize, 0)
	incrementData := activity160599BuildLTX(t, 2, 2, pageSize, 1, bytes.Repeat([]byte{0x52}, pageSize), 3, 4, WALHeaderSize, WALFrameHeaderSize+pageSize)
	snapshot := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(snapshotData))}
	deleted := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(incrementData))}

	client := &activity160599Client{}
	client.files = func(level int) []*ltx.FileInfo {
		client.mu.Lock()
		defer client.mu.Unlock()
		client.listCalls++
		if level == SnapshotLevel {
			return []*ltx.FileInfo{snapshot}
		}
		if level == 0 && !client.deleted {
			return []*ltx.FileInfo{deleted}
		}
		return nil
	}
	client.open = func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
		if level == SnapshotLevel {
			return activity160599RangeReader(snapshotData, offset, size)
		}
		client.mu.Lock()
		client.deleted = true
		client.mu.Unlock()
		return nil, fmt.Errorf("gone: %w", os.ErrNotExist)
	}

	output := filepath.Join(t.TempDir(), "missing.db")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := NewReplicaWithClient(nil, client).Restore(ctx, RestoreOptions{OutputPath: output})
	if err == nil {
		t.Fatal("expected restore failure when no plan can reach the frozen target")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restore loop was not bounded: %v", err)
	}
	client.mu.Lock()
	listCalls := client.listCalls
	client.mu.Unlock()
	if listCalls > 64 {
		t.Fatalf("excessive re-planning: %d list calls", listCalls)
	}
	activity160599AssertNoArtifacts(t, output)
}

func TestActivity160599FailedInitialRestoreNeverPublishesOrLeaksTemp(t *testing.T) {
	bad := make([]byte, ltx.HeaderSize)
	info := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(bad))}
	current := bad
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(current, offset, size)
		},
	}
	output := filepath.Join(t.TempDir(), "atomic.db")
	err := NewReplicaWithClient(nil, client).Restore(context.Background(), RestoreOptions{OutputPath: output, IntegrityCheck: IntegrityCheckQuick})
	if err == nil {
		t.Fatal("expected invalid backup to fail")
	}
	activity160599AssertNoArtifacts(t, output)

	const pageSize = 4096
	current = activity160599BuildLTX(
		t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0xA5}, pageSize),
		1, 2, WALHeaderSize, 0,
	)
	info.Size = int64(len(current))
	if err := NewReplicaWithClient(nil, client).Restore(
		context.Background(), RestoreOptions{OutputPath: output},
	); err != nil {
		t.Fatalf("clean retry to same destination: %v", err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pageSize || got[0] != 0xA5 {
		t.Fatalf("clean retry restored wrong image: size=%d first_byte=%#x", len(got), got[0])
	}
}

func TestActivity160599CancelledInitialRestoreNeverPublishesOrLeaksTemp(t *testing.T) {
	const pageSize = 4096
	data := activity160599BuildLTX(
		t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x63}, pageSize),
		1, 2, WALHeaderSize, 0,
	)
	info := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(data))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &activity160599ContextReader{ctx: ctx, entered: entered}, nil
		},
	}

	output := filepath.Join(t.TempDir(), "cancelled.db")
	errCh := make(chan error, 1)
	go func() {
		errCh <- NewReplicaWithClient(nil, client).Restore(ctx, RestoreOptions{OutputPath: output})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("restore did not begin reading before cancellation")
	}
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("cancelled restore returned success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled restore did not return promptly")
	}
	activity160599AssertNoArtifacts(t, output)
}

func TestActivity160599IntegrityFailureNeverPublishesFinalName(t *testing.T) {
	const pageSize = 4096
	data := activity160599BuildLTX(
		t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x7B}, pageSize),
		1, 2, WALHeaderSize, 0,
	)
	info := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(data))}
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(data, offset, size)
		},
	}

	dir := t.TempDir()
	output := filepath.Join(dir, "integrity.db")
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	if err := watcher.Add(dir); err != nil {
		t.Fatal(err)
	}

	err = NewReplicaWithClient(nil, client).Restore(
		context.Background(), RestoreOptions{OutputPath: output, IntegrityCheck: IntegrityCheckQuick},
	)
	if err == nil {
		t.Fatal("expected SQLite integrity check to reject invalid database image")
	}

	published := false
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
drain:
	for {
		select {
		case event := <-watcher.Events:
			if event.Name == output && event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
				published = true
			}
		case err := <-watcher.Errors:
			t.Fatalf("watch restore directory: %v", err)
		case <-timer.C:
			break drain
		}
	}
	if published {
		t.Fatal("final destination name became visible before the requested integrity check succeeded")
	}
	activity160599AssertNoArtifacts(t, output)
}

func TestActivity160599PermanentBackendFailureReturnsPromptly(t *testing.T) {
	const pageSize = 4096
	data := activity160599BuildLTX(
		t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x61}, pageSize),
		1, 2, WALHeaderSize, 0,
	)
	info := &ltx.FileInfo{
		Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(data)),
	}

	var mu sync.Mutex
	openCalls := 0
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			mu.Lock()
			defer mu.Unlock()
			openCalls++
			if openCalls == 1 {
				return activity160599ErrorReader{err: io.ErrUnexpectedEOF}, nil
			}
			return nil, os.ErrPermission
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := NewReplicaWithClient(nil, client).Restore(
		ctx, RestoreOptions{OutputPath: filepath.Join(t.TempDir(), "permanent-failure.db")},
	)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected permanent backend failure")
	}
	if errors.Is(err, context.DeadlineExceeded) || elapsed >= time.Second {
		t.Fatalf("permanent backend failure did not return promptly: elapsed=%v err=%v", elapsed, err)
	}
	mu.Lock()
	calls := openCalls
	mu.Unlock()
	if calls < 2 {
		t.Fatalf("restore returned before exercising permanent reopen failure: calls=%d err=%v", calls, err)
	}
}

func TestActivity160599ConcurrentRestoresPublishExactlyOnce(t *testing.T) {
	const pageSize = 4096
	winnerData := activity160599BuildLTX(
		t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x62}, pageSize),
		1, 2, WALHeaderSize, 0,
	)
	loserData := activity160599BuildLTX(
		t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0x73}, pageSize),
		3, 4, WALHeaderSize, 0,
	)
	info := &ltx.FileInfo{
		Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(winnerData)),
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	winnerClient := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return &activity160599BlockingReader{
				reader: bytes.NewReader(winnerData), entered: entered, release: release,
			}, nil
		},
	}
	loserClient := &activity160599Client{
		files: winnerClient.files,
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(loserData, offset, size)
		},
	}

	output := filepath.Join(t.TempDir(), "concurrent.db")
	winnerErrCh := make(chan error, 1)
	go func() {
		winnerErrCh <- NewReplicaWithClient(nil, winnerClient).Restore(
			context.Background(), RestoreOptions{OutputPath: output},
		)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("winning restore did not reach its staging read")
	}

	loserErrCh := make(chan error, 1)
	go func() {
		loserErrCh <- NewReplicaWithClient(nil, loserClient).Restore(
			context.Background(), RestoreOptions{OutputPath: output},
		)
	}()

	unblock()
	select {
	case err := <-winnerErrCh:
		if err != nil {
			t.Fatalf("winning restore: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("winning restore did not finish after release")
	}
	select {
	case err := <-loserErrCh:
		if err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("losing restore error=%v, want existing-destination refusal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("losing restore did not terminate after winner publication")
	}

	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pageSize || got[0] != 0x62 {
		t.Fatalf("loser clobbered winner: size=%d first_byte=%#x", len(got), got[0])
	}
}

func TestActivity160599CorruptFollowerUpdateIsAtomicThenRetryAdvances(t *testing.T) {
	const pageSize = 4096
	corruptInfo := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2}
	compactedInfo := &ltx.FileInfo{Level: 1, MinTXID: 2, MaxTXID: 2}
	valid := activity160599BuildLTX(t, 2, 2, pageSize, 2, bytes.Repeat([]byte{0xD4}, pageSize), 7, 8, WALHeaderSize, WALFrameHeaderSize+pageSize)
	corrupt := append([]byte(nil), valid...)
	corrupt = corrupt[:len(corrupt)-7]
	corruptInfo.Size = int64(len(valid))
	compactedInfo.Size = int64(len(valid))

	serveCompacted := false
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if !serveCompacted && level == 0 {
				return []*ltx.FileInfo{corruptInfo}
			}
			if serveCompacted && level == 1 {
				return []*ltx.FileInfo{compactedInfo}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			if level == 0 {
				return activity160599RangeReader(corrupt, offset, size)
			}
			if level == 1 {
				return activity160599RangeReader(valid, offset, size)
			}
			return nil, os.ErrNotExist
		},
	}

	path := filepath.Join(t.TempDir(), "follower.db")
	original := bytes.Repeat([]byte{0x2A}, pageSize*3)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteTXIDFile(path, 1); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	r := NewReplicaWithClient(nil, client)
	gotTXID, err := r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err == nil {
		t.Fatal("expected corrupt update to fail checksum/trailer validation")
	}
	if gotTXID != 1 {
		t.Fatalf("txid=%s after corrupt update, want 1", gotTXID)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("corrupt LTX modified the visible follower before validation completed")
	}
	if txid, err := ReadTXIDFile(path); err != nil || txid != 1 {
		t.Fatalf("sidecar txid=%s err=%v, want 1", txid, err)
	}

	serveCompacted = true
	gotTXID, err = r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err != nil {
		t.Fatalf("valid compacted retry: %v", err)
	}
	if gotTXID != 2 {
		t.Fatalf("txid=%s after valid retry, want 2", gotTXID)
	}
	if err := WriteTXIDFile(path, gotTXID); err != nil {
		t.Fatal(err)
	}
	if txid, err := ReadTXIDFile(path); err != nil || txid != 2 {
		t.Fatalf("sidecar txid=%s err=%v, want 2", txid, err)
	}
}

func TestActivity160599FollowerBatchIsAllOrNothing(t *testing.T) {
	const pageSize = 4096
	page1 := bytes.Repeat([]byte{0xB4}, pageSize)
	page2 := bytes.Repeat([]byte{0xC5}, pageSize)
	ltx2 := activity160599BuildLTX(t, 2, 2, pageSize, 3, page1, 11, 12, WALHeaderSize, WALFrameHeaderSize+pageSize)
	ltx3 := activity160599BuildLTXPages(
		t, 3, 3, pageSize, 2, 13, 14, WALHeaderSize+WALFrameHeaderSize+pageSize, WALFrameHeaderSize+pageSize,
		activity160599LTXPage{pgno: 2, data: page2},
	)
	corrupt3 := append([]byte(nil), ltx3[:len(ltx3)-9]...)
	info2 := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(ltx2))}
	info3 := &ltx.FileInfo{Level: 0, MinTXID: 3, MaxTXID: 3, Size: int64(len(ltx3))}
	serveValid3 := false
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == 0 {
				return []*ltx.FileInfo{info2, info3}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			switch maxTXID {
			case 2:
				return activity160599RangeReader(ltx2, offset, size)
			case 3:
				if serveValid3 {
					return activity160599RangeReader(ltx3, offset, size)
				}
				return activity160599RangeReader(corrupt3, offset, size)
			default:
				return nil, os.ErrNotExist
			}
		},
	}

	path := filepath.Join(t.TempDir(), "batch.db")
	original := bytes.Repeat([]byte{0x2A}, pageSize*3)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteTXIDFile(path, 1); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := NewReplicaWithClient(nil, client)

	gotTXID, err := r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err == nil {
		t.Fatal("expected the second LTX in the batch to fail validation")
	}
	if gotTXID != 1 {
		t.Fatalf("failed batch returned txid %s, want 1", gotTXID)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("the first valid LTX leaked through a later failure in the same batch")
	}
	if txid, err := ReadTXIDFile(path); err != nil || txid != 1 {
		t.Fatalf("sidecar after failed batch=%s err=%v, want 1", txid, err)
	}

	serveValid3 = true
	gotTXID, err = r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err != nil {
		t.Fatalf("retry complete batch: %v", err)
	}
	if gotTXID != 3 {
		t.Fatalf("retry txid=%s, want 3", gotTXID)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pageSize*2 || got[0] != 0xB4 || got[pageSize] != 0xC5 {
		t.Fatalf("published batch image is incomplete: size=%d page1=%#x page2=%#x", len(got), got[0], got[pageSize])
	}
	if txid, err := ReadTXIDFile(path); err != nil || txid != 3 {
		t.Fatalf("sidecar after successful batch=%s err=%v, want 3", txid, err)
	}
	activity160599AssertNoFollowArtifacts(t, path)
}

func TestActivity160599FollowerCommitRecoveryIsIdentityChecked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tamper bool
	}{
		{name: "roll-forward", tamper: false},
		{name: "reject-tampered-generation", tamper: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const pageSize = 4096
			update := activity160599BuildLTX(t, 2, 2, pageSize, 1, bytes.Repeat([]byte{0xD6}, pageSize), 21, 22, WALHeaderSize, WALFrameHeaderSize+pageSize)
			info := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(update))}
			client := &activity160599Client{
				files: func(level int) []*ltx.FileInfo {
					if level == 0 {
						return []*ltx.FileInfo{info}
					}
					return nil
				},
				open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
					return activity160599RangeReader(update, offset, size)
				},
			}

			path := filepath.Join(t.TempDir(), "recover.db")
			oldGeneration := bytes.Repeat([]byte{0x25}, pageSize)
			if err := os.WriteFile(path, oldGeneration, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := WriteTXIDFile(path, 1); err != nil {
				t.Fatal(err)
			}
			blocker := TXIDPath(path) + ".tmp"
			if err := os.Mkdir(blocker, 0o700); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			r := NewReplicaWithClient(nil, client)

			gotTXID, err := r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
			if err == nil {
				t.Fatal("blocked TXID publication unexpectedly succeeded")
			}
			if gotTXID != 1 {
				t.Fatalf("failed publication returned txid %s, want 1", gotTXID)
			}
			visible, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(visible) != pageSize || visible[0] != 0xD6 {
				t.Fatalf("database generation was not published before injected sidecar failure: size=%d first=%#x", len(visible), visible[0])
			}
			if txid, err := ReadTXIDFile(path); err != nil || txid != 1 {
				t.Fatalf("old sidecar changed during failed publication: txid=%s err=%v", txid, err)
			}
			if _, err := os.Stat(path + "-follow"); err != nil {
				t.Fatalf("durable recovery record missing: %v", err)
			}
			record := activity160599ReadFollowRecord(t, path)
			oldSHA := sha256.Sum256(oldGeneration)
			newSHA := sha256.Sum256(visible)
			if record.Version != 1 || record.FromTXID != "0000000000000001" || record.ToTXID != "0000000000000002" {
				t.Fatalf("follow record version/range=%+v, want v1 1..2", record)
			}
			if record.OldSize != pageSize || record.NewSize != pageSize || record.OldSHA256 != fmt.Sprintf("%x", oldSHA) || record.NewSHA256 != fmt.Sprintf("%x", newSHA) {
				t.Fatalf("follow record identities=%+v, want exact old/new size and SHA-256", record)
			}
			stagePrefix := "." + filepath.Base(path) + "-follow-"
			if filepath.Base(record.Stage) != record.Stage || !strings.HasPrefix(record.Stage, stagePrefix) {
				t.Fatalf("follow record stage=%q, want basename with prefix %q", record.Stage, stagePrefix)
			}

			if tc.tamper {
				visible[100] ^= 0xff
				if err := os.WriteFile(path, visible, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}

			gotTXID, err = r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
			if tc.tamper {
				if err == nil || !strings.Contains(err.Error(), "neither") {
					t.Fatalf("tampered recovery error=%v, want fail-closed identity mismatch", err)
				}
				if gotTXID != 1 {
					t.Fatalf("tampered recovery returned txid %s, want 1", gotTXID)
				}
				if txid, err := ReadTXIDFile(path); err != nil || txid != 1 {
					t.Fatalf("tampered recovery advanced sidecar: txid=%s err=%v", txid, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("recover interrupted commit: %v", err)
			}
			if gotTXID != 2 {
				t.Fatalf("recovered txid=%s, want 2", gotTXID)
			}
			if txid, err := ReadTXIDFile(path); err != nil || txid != 2 {
				t.Fatalf("recovered sidecar=%s err=%v, want 2", txid, err)
			}
			activity160599AssertNoFollowArtifacts(t, path)
		})
	}
}

func TestActivity160599InitialFollowPublicationRecoversMissingTXID(t *testing.T) {
	const pageSize = 4096
	page := bytes.Repeat([]byte{0x46}, pageSize)
	page[16], page[17] = 0x10, 0x00
	snapshotData := activity160599BuildLTX(t, 1, 1, pageSize, 1, page, 31, 32, WALHeaderSize, 0)
	info := &ltx.FileInfo{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(snapshotData))}
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(snapshotData, offset, size)
		},
	}

	path := filepath.Join(t.TempDir(), "initial-follow.db")
	blocker := TXIDPath(path) + ".tmp"
	if err := os.Mkdir(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	r := NewReplicaWithClient(nil, client)
	err := r.Restore(context.Background(), RestoreOptions{OutputPath: path, Follow: true, FollowInterval: time.Millisecond})
	if err == nil {
		t.Fatal("initial follow restore unexpectedly survived blocked TXID publication")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("published database missing after injected crash window: %v", statErr)
	}
	if _, statErr := os.Stat(path + "-follow"); statErr != nil {
		t.Fatalf("initial recovery record missing: %v", statErr)
	}
	record := activity160599ReadFollowRecord(t, path)
	visible, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	newSHA := sha256.Sum256(visible)
	if record.Version != 1 || record.FromTXID != "0000000000000000" || record.ToTXID != "0000000000000001" {
		t.Fatalf("initial follow record version/range=%+v, want v1 0..1", record)
	}
	if record.OldSize != -1 || record.OldSHA256 != "" || record.NewSize != int64(len(visible)) || record.NewSHA256 != fmt.Sprintf("%x", newSHA) {
		t.Fatalf("initial follow record identities=%+v, want absent old and exact new identity", record)
	}
	stagePrefix := "." + filepath.Base(path) + "-restore-"
	if filepath.Base(record.Stage) != record.Stage || !strings.HasPrefix(record.Stage, stagePrefix) {
		t.Fatalf("initial follow stage=%q, want basename with prefix %q", record.Stage, stagePrefix)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Restore(ctx, RestoreOptions{OutputPath: path, Follow: true, FollowInterval: time.Millisecond}); err != nil {
		t.Fatalf("restart did not reconcile initial database/TXID pair: %v", err)
	}
	if txid, err := ReadTXIDFile(path); err != nil || txid != 1 {
		t.Fatalf("recovered initial sidecar=%s err=%v, want 1", txid, err)
	}
	activity160599AssertNoFollowArtifacts(t, path)
}

func TestActivity160599FollowRecoveryRejectsPathTraversalAndMalformedState(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(map[string]any)
		trimNewline bool
		sidecar     ltx.TXID
	}{
		{
			name: "path-traversing-stage",
			mutate: func(record map[string]any) {
				record["stage"] = "../outside.db"
			},
		},
		{
			name: "missing-required-field",
			mutate: func(record map[string]any) {
				delete(record, "old_size")
			},
		},
		{
			name: "unknown-extra-field",
			mutate: func(record map[string]any) {
				record["unexpected"] = true
			},
		},
		{
			name: "wrong-field-type",
			mutate: func(record map[string]any) {
				record["old_size"] = "4096"
			},
		},
		{
			name: "inconsistent-old-identity",
			mutate: func(record map[string]any) {
				record["old_size"] = -1
				record["old_sha256"] = ""
			},
		},
		{
			name: "non-lowercase-sha256",
			mutate: func(record map[string]any) {
				record["new_sha256"] = strings.Repeat("A", sha256.Size*2)
			},
		},
		{
			name:    "non-lowercase-from-txid",
			sidecar: 10,
			mutate: func(record map[string]any) {
				record["from_txid"] = "000000000000000A"
				record["to_txid"] = "000000000000000b"
			},
		},
		{
			name:    "non-lowercase-to-txid",
			sidecar: 10,
			mutate: func(record map[string]any) {
				record["from_txid"] = "000000000000000a"
				record["to_txid"] = "000000000000000B"
			},
		},
		{
			name:        "unterminated-json-record",
			trimNewline: true,
		},
		{
			name: "restore-stage-on-later-poll",
			mutate: func(record map[string]any) {
				record["stage"] = ".recover.db-restore-wrong-generation"
			},
		},
		{
			name:    "impossible-sidecar",
			sidecar: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const pageSize = 4096
			dir := t.TempDir()
			path := filepath.Join(dir, "recover.db")
			visible := bytes.Repeat([]byte{0x31}, pageSize)
			if err := os.WriteFile(path, visible, 0o600); err != nil {
				t.Fatal(err)
			}
			sidecar := tc.sidecar
			if sidecar == 0 {
				sidecar = 1
			}
			if err := WriteTXIDFile(path, sidecar); err != nil {
				t.Fatal(err)
			}

			oldSHA := sha256.Sum256(visible)
			record := map[string]any{
				"version":    1,
				"from_txid":  "0000000000000001",
				"to_txid":    "0000000000000002",
				"stage":      ".recover.db-follow-stage",
				"old_size":   pageSize,
				"old_sha256": fmt.Sprintf("%x", oldSHA),
				"new_size":   pageSize,
				"new_sha256": strings.Repeat("2", sha256.Size*2),
			}
			if tc.mutate != nil {
				tc.mutate(record)
			}
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.trimNewline {
				data = append(data, '\n')
			}
			if err := os.WriteFile(path+"-follow", data, 0o600); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Clean(filepath.Join(dir, "../outside.db"))
			outsideData := []byte("path traversal sentinel")
			if err := os.WriteFile(outside, outsideData, 0o600); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = NewReplicaWithClient(nil, &activity160599Client{}).Restore(
				ctx, RestoreOptions{OutputPath: path, Follow: true, FollowInterval: time.Millisecond},
			)
			if err == nil {
				t.Fatal("malformed or path-traversing follow state was accepted")
			}
			got, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !bytes.Equal(got, visible) {
				t.Fatal("failed-closed recovery modified the visible database")
			}
			if txid, readErr := ReadTXIDFile(path); readErr != nil || txid != sidecar {
				t.Fatalf("failed-closed recovery changed sidecar: txid=%s err=%v", txid, readErr)
			}
			if gotOutside, readErr := os.ReadFile(outside); readErr != nil || !bytes.Equal(gotOutside, outsideData) {
				t.Fatalf("path traversal touched outside sentinel: data=%q err=%v", gotOutside, readErr)
			}
			if _, statErr := os.Stat(path + "-follow"); statErr != nil {
				t.Fatalf("failed-closed recovery removed evidence record: %v", statErr)
			}
		})
	}
}

func TestActivity160599FollowerUpdatePreservesFileMode(t *testing.T) {
	const pageSize = 4096
	update := activity160599BuildLTX(t, 2, 2, pageSize, 1, bytes.Repeat([]byte{0x65}, pageSize), 41, 42, WALHeaderSize, WALFrameHeaderSize+pageSize)
	info := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(update))}
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == 0 {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(update, offset, size)
		},
	}

	path := filepath.Join(t.TempDir(), "mode.db")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x19}, pageSize), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := WriteTXIDFile(path, 1); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gotTXID, err := NewReplicaWithClient(nil, client).applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err != nil {
		t.Fatalf("apply follower update: %v", err)
	}
	if gotTXID != 2 {
		t.Fatalf("updated txid=%s, want 2", gotTXID)
	}
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := stat.Mode().Perm(); got != 0o640 {
		t.Fatalf("follower mode=%#o, want %#o", got, os.FileMode(0o640))
	}
}

func TestActivity160599FollowerUsesHigherLevelGapFill(t *testing.T) {
	const pageSize = 4096
	page1 := bytes.Repeat([]byte{0x72}, pageSize)
	page2 := bytes.Repeat([]byte{0x84}, pageSize)
	level1Data := activity160599BuildLTX(t, 2, 3, pageSize, 2, page1, 51, 52, WALHeaderSize, 2*(WALFrameHeaderSize+pageSize))
	level0Data := activity160599BuildLTXPages(
		t, 4, 4, pageSize, 2, 53, 54, WALHeaderSize+2*(WALFrameHeaderSize+pageSize), WALFrameHeaderSize+pageSize,
		activity160599LTXPage{pgno: 2, data: page2},
	)
	level1 := &ltx.FileInfo{Level: 1, MinTXID: 2, MaxTXID: 3, Size: int64(len(level1Data))}
	level0 := &ltx.FileInfo{Level: 0, MinTXID: 4, MaxTXID: 4, Size: int64(len(level0Data))}
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			switch level {
			case 0:
				return []*ltx.FileInfo{level0}
			case 1:
				return []*ltx.FileInfo{level1}
			default:
				return nil
			}
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			switch activity160599Key(level, minTXID, maxTXID) {
			case activity160599Key(1, 2, 3):
				return activity160599RangeReader(level1Data, offset, size)
			case activity160599Key(0, 4, 4):
				return activity160599RangeReader(level0Data, offset, size)
			default:
				return nil, os.ErrNotExist
			}
		},
	}

	path := filepath.Join(t.TempDir(), "gap.db")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x18}, pageSize*2), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteTXIDFile(path, 1); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gotTXID, err := NewReplicaWithClient(nil, client).applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err != nil {
		t.Fatalf("apply gap-filled poll: %v", err)
	}
	if gotTXID != 4 {
		t.Fatalf("gap-filled txid=%s, want 4", gotTXID)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != pageSize*2 || got[0] != 0x72 || got[pageSize] != 0x84 {
		t.Fatalf("gap-filled image mismatch: size=%d page1=%#x page2=%#x", len(got), got[0], got[pageSize])
	}
	if txid, err := ReadTXIDFile(path); err != nil || txid != 4 {
		t.Fatalf("gap-filled sidecar=%s err=%v, want 4", txid, err)
	}
	activity160599AssertNoFollowArtifacts(t, path)
}

type activity160599Client struct {
	mu        sync.Mutex
	deleted   bool
	listCalls int
	files     func(level int) []*ltx.FileInfo
	open      func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}

type activity160599FollowRecord struct {
	Version   int    `json:"version"`
	FromTXID  string `json:"from_txid"`
	ToTXID    string `json:"to_txid"`
	Stage     string `json:"stage"`
	OldSize   int64  `json:"old_size"`
	OldSHA256 string `json:"old_sha256"`
	NewSize   int64  `json:"new_size"`
	NewSHA256 string `json:"new_sha256"`
}

func activity160599ReadFollowRecord(tb testing.TB, output string) activity160599FollowRecord {
	tb.Helper()
	data, err := os.ReadFile(output + "-follow")
	if err != nil {
		tb.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		tb.Fatalf("decode follow record JSON: %v", err)
	}
	if len(raw) != 8 {
		tb.Fatalf("follow record has %d fields, want exactly 8", len(raw))
	}
	for _, key := range []string{"version", "from_txid", "to_txid", "stage", "old_size", "old_sha256", "new_size", "new_sha256"} {
		if _, ok := raw[key]; !ok {
			tb.Fatalf("follow record missing field %q", key)
		}
	}
	var record activity160599FollowRecord
	if err := json.Unmarshal(data, &record); err != nil {
		tb.Fatalf("decode typed follow record: %v", err)
	}
	return record
}

type activity160599ErrorReader struct {
	err error
}

func (r activity160599ErrorReader) Read([]byte) (int, error) { return 0, r.err }
func (activity160599ErrorReader) Close() error               { return nil }

type activity160599FaultReader struct {
	data        []byte
	cut         int
	pos         int
	terminalErr error
}

func (r *activity160599FaultReader) Read(p []byte) (int, error) {
	if r.pos >= r.cut {
		return 0, r.terminalErr
	}
	n := len(p)
	if remaining := r.cut - r.pos; n > remaining {
		n = remaining
	}
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	if r.pos == r.cut {
		return n, r.terminalErr
	}
	return n, nil
}

func (*activity160599FaultReader) Close() error { return nil }

type activity160599BlockingReader struct {
	once    sync.Once
	reader  *bytes.Reader
	entered chan<- struct{}
	release <-chan struct{}
}

func (r *activity160599BlockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() {
		r.entered <- struct{}{}
		<-r.release
	})
	return r.reader.Read(p)
}

func (*activity160599BlockingReader) Close() error { return nil }

type activity160599ContextReader struct {
	ctx     context.Context
	entered chan<- struct{}
	once    sync.Once
}

func (r *activity160599ContextReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (*activity160599ContextReader) Close() error { return nil }

func (*activity160599Client) Type() string                                          { return "activity160599" }
func (*activity160599Client) Init(context.Context) error                            { return nil }
func (*activity160599Client) SetLogger(*slog.Logger)                                {}
func (*activity160599Client) DeleteAll(context.Context) error                       { return nil }
func (*activity160599Client) DeleteLTXFiles(context.Context, []*ltx.FileInfo) error { return nil }

func (c *activity160599Client) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	var infos []*ltx.FileInfo
	if c.files != nil {
		infos = c.files(level)
	}
	filtered := make([]*ltx.FileInfo, 0, len(infos))
	for _, info := range infos {
		if seek == 0 || info.MaxTXID >= seek {
			clone := *info
			filtered = append(filtered, &clone)
		}
	}
	return ltx.NewFileInfoSliceIterator(filtered), nil
}

func (c *activity160599Client) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if c.open == nil {
		return nil, os.ErrNotExist
	}
	return c.open(level, minTXID, maxTXID, offset, size)
}

func (*activity160599Client) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, errors.New("unexpected write")
}

type activity160599LTXPage struct {
	pgno uint32
	data []byte
}

func activity160599BuildLTX(tb testing.TB, minTXID, maxTXID ltx.TXID, pageSize, commit uint32, page []byte, salt1, salt2 uint32, walOffset, walSize int64) []byte {
	return activity160599BuildLTXPages(
		tb, minTXID, maxTXID, pageSize, commit, salt1, salt2, walOffset, walSize,
		activity160599LTXPage{pgno: 1, data: page},
	)
}

func activity160599BuildLTXPages(tb testing.TB, minTXID, maxTXID ltx.TXID, pageSize, commit uint32, salt1, salt2 uint32, walOffset, walSize int64, pages ...activity160599LTXPage) []byte {
	tb.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatal(err)
	}
	hdr := ltx.Header{
		Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: pageSize,
		Commit: commit, MinTXID: minTXID, MaxTXID: maxTXID, Timestamp: time.Now().UnixMilli(),
		WALSalt1: salt1, WALSalt2: salt2, WALOffset: walOffset, WALSize: walSize,
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		tb.Fatal(err)
	}
	for _, page := range pages {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: page.pgno}, page.data); err != nil {
			tb.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func activity160599RangeReader(data []byte, offset, size int64) (io.ReadCloser, error) {
	if offset < 0 || offset > int64(len(data)) {
		return nil, io.ErrUnexpectedEOF
	}
	end := int64(len(data))
	if size > 0 && offset+size < end {
		end = offset + size
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), nil
}

func activity160599BuildWAL(tb testing.TB, pageSize int, salt1, salt2 uint32, page []byte) []byte {
	tb.Helper()
	if len(page) != pageSize {
		tb.Fatalf("WAL page size=%d, want %d", len(page), pageSize)
	}
	bo := binary.LittleEndian
	wal := make([]byte, WALHeaderSize+WALFrameHeaderSize+pageSize)
	header := wal[:WALHeaderSize]
	binary.BigEndian.PutUint32(header[0:], 0x377f0682)
	binary.BigEndian.PutUint32(header[4:], 3007000)
	binary.BigEndian.PutUint32(header[8:], uint32(pageSize))
	binary.BigEndian.PutUint32(header[12:], 1)
	binary.BigEndian.PutUint32(header[16:], salt1)
	binary.BigEndian.PutUint32(header[20:], salt2)
	s0, s1 := activity160599WALChecksum(bo, 0, 0, header[:24])
	binary.BigEndian.PutUint32(header[24:], s0)
	binary.BigEndian.PutUint32(header[28:], s1)

	frame := wal[WALHeaderSize : WALHeaderSize+WALFrameHeaderSize]
	binary.BigEndian.PutUint32(frame[0:], 1)
	binary.BigEndian.PutUint32(frame[4:], 1)
	binary.BigEndian.PutUint32(frame[8:], salt1)
	binary.BigEndian.PutUint32(frame[12:], salt2)
	copy(wal[WALHeaderSize+WALFrameHeaderSize:], page)
	s0, s1 = activity160599WALChecksum(bo, s0, s1, frame[:8])
	s0, s1 = activity160599WALChecksum(bo, s0, s1, page)
	binary.BigEndian.PutUint32(frame[16:], s0)
	binary.BigEndian.PutUint32(frame[20:], s1)
	return wal
}

func activity160599WALChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	if len(b)%8 != 0 {
		panic("misaligned checksum byte slice")
	}
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

func activity160599AssertNoArtifacts(tb testing.TB, output string) {
	tb.Helper()
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		tb.Fatalf("output became visible after failed restore: err=%v", err)
	}
	matches, err := filepath.Glob(output + ".tmp*")
	if err != nil {
		tb.Fatal(err)
	}
	if len(matches) != 0 {
		tb.Fatalf("temporary restore artifacts leaked: %v", matches)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(output), "."+filepath.Base(output)+"-restore-*")); err != nil {
		tb.Fatal(err)
	} else if len(matches) != 0 {
		tb.Fatalf("unique restore artifacts leaked: %v", matches)
	}
	for _, path := range []string{TXIDPath(output), TXIDPath(output) + ".tmp", output + "-shm", output + "-wal"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			tb.Fatalf("restore side artifact leaked: path=%s err=%v", path, err)
		}
	}
}

func activity160599AssertNoFollowArtifacts(tb testing.TB, output string) {
	tb.Helper()
	for _, path := range []string{output + "-follow", TXIDPath(output) + ".tmp"} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			tb.Fatalf("follow publication artifact leaked: path=%s err=%v", path, err)
		}
	}
	patterns := []string{
		filepath.Join(filepath.Dir(output), "."+filepath.Base(output)+"-follow-*"),
		filepath.Join(filepath.Dir(output), "."+filepath.Base(output)+"-follow-record-*"),
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			tb.Fatal(err)
		}
		if len(matches) != 0 {
			tb.Fatalf("follow staging artifacts leaked for %s: %v", pattern, matches)
		}
	}
}

func activity160599Key(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%d:%s:%s", level, minTXID, maxTXID)
}
