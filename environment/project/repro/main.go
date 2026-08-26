package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	litestream "github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/superfly/ltx"
)

type result struct {
	Scenario string `json:"scenario"`
	OK       bool   `json:"ok"`
	TXID     string `json:"txid"`
	Detail   string `json:"detail"`
}

func main() {
	results := []result{
		runExactResume(),
		runBoundedResume(),
		runRetentionReplan(),
		runFailureCleanup(),
		runInitialFollowRecovery(),
	}
	ok := true
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	for _, r := range results {
		if !r.OK {
			ok = false
		}
		_ = enc.Encode(r)
	}
	if !ok {
		os.Exit(1)
	}
}

func runBoundedResume() result {
	data := bytes.Repeat([]byte("broken-range-"), 8)
	opener := &oneByteFaultOpener{data: data}
	r := internal.NewResumableReader(
		context.Background(), opener, 0, 7, 7, int64(len(data)),
		&oneFaultReader{data: data, cut: 1},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	got, err := io.ReadAll(r)
	ok := errors.Is(err, io.ErrUnexpectedEOF) && len(got) == 4 && len(opener.offsets) == 3
	detail := "retry budget stayed bounded across partial-read calls"
	if !ok {
		detail = fmt.Sprintf("err=%v bytes=%d offsets=%v", err, len(got), opener.offsets)
	}
	return result{Scenario: "resume_budget", OK: ok, TXID: "7", Detail: detail}
}

func runExactResume() result {
	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	opener := &rangeClient{data: data}
	r := internal.NewResumableReader(
		context.Background(), opener, 0, 1, 1, int64(len(data)),
		&oneFaultReader{data: data, cut: 13},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	got, err := io.ReadAll(r)
	ok := err == nil && bytes.Equal(got, data) && len(opener.offsets) == 1 && opener.offsets[0] == 13
	detail := "range reopen preserved every byte"
	if !ok {
		detail = fmt.Sprintf("err=%v bytes=%d offsets=%v", err, len(got), opener.offsets)
	}
	return result{Scenario: "resume_exact", OK: ok, TXID: "1", Detail: detail}
}

func runRetentionReplan() result {
	const pageSize = 4096
	snapData := buildLTX(1, 1, bytes.Repeat([]byte{0x61}, pageSize))
	incData := buildLTX(2, 2, bytes.Repeat([]byte{0x62}, pageSize))
	snapshot := &ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(snapData))}
	deleted := &ltx.FileInfo{Level: 0, MinTXID: 2, MaxTXID: 2, Size: int64(len(incData))}
	alternative := &ltx.FileInfo{Level: 1, MinTXID: 2, MaxTXID: 2, Size: int64(len(incData))}

	c := &replicaClient{}
	c.files = func(level int) []*ltx.FileInfo {
		c.mu.Lock()
		defer c.mu.Unlock()
		if level == litestream.SnapshotLevel {
			return []*ltx.FileInfo{snapshot}
		}
		if level == 0 && !c.deleted {
			return []*ltx.FileInfo{deleted}
		}
		if level == 1 && c.deleted {
			return []*ltx.FileInfo{alternative}
		}
		return nil
	}
	c.open = func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
		switch {
		case level == litestream.SnapshotLevel && minTXID == 1:
			return sliceReader(snapData, offset, size)
		case level == 0 && minTXID == 2:
			c.mu.Lock()
			defer c.mu.Unlock()
			if offset == 0 && !c.deleted {
				return &oneFaultReader{data: incData, cut: ltx.HeaderSize + 37}, nil
			}
			c.deleted = true
			return nil, fmt.Errorf("retained range disappeared: %w", os.ErrNotExist)
		case level == 1 && minTXID == 2:
			return sliceReader(incData, offset, size)
		default:
			return nil, os.ErrNotExist
		}
	}

	dir, _ := os.MkdirTemp("", "litestream-repro-")
	defer os.RemoveAll(dir)
	output := filepath.Join(dir, "restored.db")
	err := litestream.NewReplicaWithClient(nil, c).Restore(context.Background(), litestream.RestoreOptions{OutputPath: output})
	got, readErr := os.ReadFile(output)
	ok := err == nil && readErr == nil && len(got) == pageSize && got[0] == 0x62
	detail := "restore replanned to the same transaction"
	if !ok {
		detail = fmt.Sprintf("restore=%v read=%v size=%d", err, readErr, len(got))
	}
	return result{Scenario: "retention_replan", OK: ok, TXID: "2", Detail: detail}
}

func runInitialFollowRecovery() result {
	const pageSize = 4096
	page := bytes.Repeat([]byte{0x45}, pageSize)
	page[16], page[17] = 0x10, 0x00
	snapshotData := buildLTX(1, 1, page)
	info := &ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(snapshotData))}
	c := &replicaClient{
		files: func(level int) []*ltx.FileInfo {
			if level == litestream.SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(_ int, _, _ ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return sliceReader(snapshotData, offset, size)
		},
	}
	dir, _ := os.MkdirTemp("", "litestream-repro-")
	defer os.RemoveAll(dir)
	output := filepath.Join(dir, "follow.db")
	blocker := litestream.TXIDPath(output) + ".tmp"
	_ = os.Mkdir(blocker, 0o700)
	r := litestream.NewReplicaWithClient(nil, c)
	firstErr := r.Restore(context.Background(), litestream.RestoreOptions{OutputPath: output, Follow: true, FollowInterval: time.Millisecond})
	_ = os.Remove(blocker)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	restartErr := r.Restore(ctx, litestream.RestoreOptions{OutputPath: output, Follow: true, FollowInterval: time.Millisecond})
	txid, txidErr := litestream.ReadTXIDFile(output)
	_, markerErr := os.Stat(output + "-follow")
	ok := firstErr != nil && restartErr == nil && txidErr == nil && txid == 1 && errors.Is(markerErr, os.ErrNotExist)
	detail := "restart reconciled the published database with its TXID"
	if !ok {
		detail = fmt.Sprintf("first=%v restart=%v txid=%s txid_err=%v marker=%v", firstErr, restartErr, txid, txidErr, markerErr)
	}
	return result{Scenario: "initial_follow_recovery", OK: ok, TXID: txid.String(), Detail: detail}
}

func runFailureCleanup() result {
	bad := make([]byte, ltx.HeaderSize)
	info := &ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 1, Size: int64(len(bad))}
	c := &replicaClient{
		files: func(level int) []*ltx.FileInfo {
			if level == litestream.SnapshotLevel {
				return []*ltx.FileInfo{info}
			}
			return nil
		},
		open: func(_ int, _, _ ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return sliceReader(bad, offset, size)
		},
	}
	dir, _ := os.MkdirTemp("", "litestream-repro-")
	defer os.RemoveAll(dir)
	output := filepath.Join(dir, "failed.db")
	err := litestream.NewReplicaWithClient(nil, c).Restore(context.Background(), litestream.RestoreOptions{OutputPath: output})
	_, finalErr := os.Stat(output)
	temps, _ := filepath.Glob(output + ".tmp*")
	ok := err != nil && errors.Is(finalErr, os.ErrNotExist) && len(temps) == 0
	detail := "failed restore left no visible or temporary file"
	if !ok {
		detail = fmt.Sprintf("restore=%v output=%v temps=%d", err, finalErr, len(temps))
	}
	return result{Scenario: "failure_cleanup", OK: ok, TXID: "0", Detail: detail}
}

type oneFaultReader struct {
	data []byte
	cut  int
	pos  int
	done bool
}

func (r *oneFaultReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	remaining := r.cut - r.pos
	if remaining <= 0 {
		r.done = true
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	n := copy(p, r.data[r.pos:r.cut])
	r.pos += n
	if r.pos == r.cut {
		r.done = true
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}
func (*oneFaultReader) Close() error { return nil }

type rangeClient struct {
	data    []byte
	offsets []int64
}

type oneByteFaultOpener struct {
	data    []byte
	offsets []int64
}

func (c *oneByteFaultOpener) OpenLTXFile(_ context.Context, _ int, _, _ ltx.TXID, offset, _ int64) (io.ReadCloser, error) {
	c.offsets = append(c.offsets, offset)
	if offset < 0 || offset >= int64(len(c.data)) {
		return &oneFaultReader{}, nil
	}
	return &oneFaultReader{data: c.data[offset:], cut: 1}, nil
}

func (c *rangeClient) OpenLTXFile(_ context.Context, _ int, _, _ ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.offsets = append(c.offsets, offset)
	return sliceReader(c.data, offset, size)
}

type replicaClient struct {
	mu      sync.Mutex
	deleted bool
	files   func(level int) []*ltx.FileInfo
	open    func(level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}

func (*replicaClient) Type() string                                          { return "repro" }
func (*replicaClient) Init(context.Context) error                            { return nil }
func (*replicaClient) SetLogger(*slog.Logger)                                {}
func (*replicaClient) DeleteAll(context.Context) error                       { return nil }
func (*replicaClient) DeleteLTXFiles(context.Context, []*ltx.FileInfo) error { return nil }
func (*replicaClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, errors.New("unexpected write")
}
func (c *replicaClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	infos := c.files(level)
	var out []*ltx.FileInfo
	for _, info := range infos {
		if seek == 0 || info.MaxTXID >= seek {
			clone := *info
			out = append(out, &clone)
		}
	}
	return ltx.NewFileInfoSliceIterator(out), nil
}
func (c *replicaClient) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	return c.open(level, minTXID, maxTXID, offset, size)
}

func buildLTX(minTXID, maxTXID ltx.TXID, page []byte) []byte {
	var buf bytes.Buffer
	enc, _ := ltx.NewEncoder(&buf)
	_ = enc.EncodeHeader(ltx.Header{
		Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: uint32(len(page)), Commit: 1,
		MinTXID: minTXID, MaxTXID: maxTXID, Timestamp: time.Now().UnixMilli(),
	})
	_ = enc.EncodePage(ltx.PageHeader{Pgno: 1}, page)
	_ = enc.Close()
	return buf.Bytes()
}

func sliceReader(data []byte, offset, size int64) (io.ReadCloser, error) {
	if offset < 0 || offset > int64(len(data)) {
		return nil, io.ErrUnexpectedEOF
	}
	end := int64(len(data))
	if size > 0 && offset+size < end {
		end = offset + size
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), nil
}
