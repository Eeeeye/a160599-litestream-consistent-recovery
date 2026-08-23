package litestream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestActivity160599SameSizeWALRecycleIsNotIdle(t *testing.T) {
	const (
		pageSize = 4096
		oldSalt1 = 0x11111111
		oldSalt2 = 0x22222222
		newSalt1 = 0x33333333
		newSalt2 = 0x44444444
	)

	dir := t.TempDir()
	db := NewDB(filepath.Join(dir, "source.db"))
	ltxData := activity160599BuildLTX(t, 1, 1, pageSize, 1, bytes.Repeat([]byte{0xA1}, pageSize), oldSalt1, oldSalt2, WALHeaderSize, WALFrameHeaderSize+pageSize)
	ltxPath := db.LTXPath(0, 1, 1)
	if err := os.MkdirAll(filepath.Dir(ltxPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ltxPath, ltxData, 0o600); err != nil {
		t.Fatal(err)
	}

	wal := make([]byte, WALHeaderSize+WALFrameHeaderSize+pageSize)
	binary.BigEndian.PutUint32(wal[16:], newSalt1)
	binary.BigEndian.PutUint32(wal[20:], newSalt2)
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
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(bad, offset, size)
		},
	}
	output := filepath.Join(t.TempDir(), "atomic.db")
	err := NewReplicaWithClient(nil, client).Restore(context.Background(), RestoreOptions{OutputPath: output, IntegrityCheck: IntegrityCheckQuick})
	if err == nil {
		t.Fatal("expected invalid backup to fail")
	}
	activity160599AssertNoArtifacts(t, output)
}

func TestActivity160599CorruptFollowerUpdateIsAtomicThenRetryAdvances(t *testing.T) {
	const pageSize = 4096
	info := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2}
	valid := activity160599BuildLTX(t, 2, 2, pageSize, 2, bytes.Repeat([]byte{0xD4}, pageSize), 7, 8, WALHeaderSize, WALFrameHeaderSize+pageSize)
	corrupt := append([]byte(nil), valid...)
	corrupt = corrupt[:len(corrupt)-7]
	info.Size = int64(len(valid))

	current := corrupt
	client := &activity160599Client{
		files: func(level int) []*ltx.FileInfo {
			if level == 0 {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return activity160599RangeReader(current, offset, size)
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

	current = valid
	gotTXID, err = r.applyNewLTXFiles(context.Background(), f, 1, pageSize)
	if err != nil {
		t.Fatalf("valid retry: %v", err)
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

type activity160599Client struct {
	mu        sync.Mutex
	deleted   bool
	listCalls int
	files     func(level int) []*ltx.FileInfo
	open      func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}

func (*activity160599Client) Type() string                 { return "activity160599" }
func (*activity160599Client) Init(context.Context) error   { return nil }
func (*activity160599Client) SetLogger(*slog.Logger)       {}
func (*activity160599Client) DeleteAll(context.Context) error { return nil }
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

func activity160599BuildLTX(tb testing.TB, minTXID, maxTXID ltx.TXID, pageSize, commit uint32, page []byte, salt1, salt2 uint32, walOffset, walSize int64) []byte {
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
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page); err != nil {
		tb.Fatal(err)
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
}

func activity160599Key(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%d:%s:%s", level, minTXID, maxTXID)
}
