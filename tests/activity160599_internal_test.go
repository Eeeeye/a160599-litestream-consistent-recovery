package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestActivity160599ResumableReaderIsByteExact(t *testing.T) {
	payload := make([]byte, 12289)
	for i := range payload {
		payload[i] = byte((i*31 + 7) % 251)
	}

	for _, failure := range []struct {
		name string
		err  error
	}{
		{name: "unexpected-eof", err: io.ErrUnexpectedEOF},
		{name: "clean-eof", err: io.EOF},
		{name: "connection-reset", err: errors.New("connection reset by peer")},
	} {
		for _, cut := range []int{0, 1, 31, 511, 4096, 8193, len(payload) - 1} {
			t.Run(failure.name+"/"+ltx.TXID(cut).String(), func(t *testing.T) {
				opener := &activity160599Opener{data: payload}
				r := NewResumableReader(
					context.Background(), opener, 2, 11, 17, int64(len(payload)),
					&activity160599FaultReader{data: payload, cut: cut, terminalErr: failure.err},
					slog.New(slog.NewTextHandler(io.Discard, nil)),
				)

				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("read all after interruption at %d: %v", cut, err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("stream changed after interruption at %d: got %d bytes, want %d", cut, len(got), len(payload))
				}
				if offsets := opener.offsetsSnapshot(); len(offsets) != 1 || offsets[0] != int64(cut) {
					t.Fatalf("range reopen offsets=%v, want [%d]", offsets, cut)
				}
			})
		}
	}
}

func TestActivity160599ResumableReaderHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opener := &activity160599Opener{data: []byte("abcdef"), err: context.Canceled}
	r := NewResumableReader(
		ctx, opener, 0, 1, 1, 6,
		&activity160599FaultReader{data: []byte("abcdef"), cut: 0, terminalErr: io.ErrUnexpectedEOF},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(r)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context cancellation", err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("reader did not fail promptly after cancellation")
	}
}

func TestActivity160599ResumableReaderBudgetSpansReadCallsAndIsSticky(t *testing.T) {
	payload := bytes.Repeat([]byte("range-fragment-"), 32)
	opener := &activity160599OneByteFaultOpener{data: payload}
	r := NewResumableReader(
		context.Background(), opener, 0, 9, 9, int64(len(payload)),
		&activity160599FaultReader{data: payload, cut: 1, terminalErr: io.ErrUnexpectedEOF},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	got, err := io.ReadAll(r)
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read error=%v, want bounded unexpected-EOF failure", err)
	}
	if len(got) != resumableReaderMaxRetries+1 {
		t.Fatalf("reader consumed %d bytes across broken streams, want %d", len(got), resumableReaderMaxRetries+1)
	}
	offsets := opener.offsetsSnapshot()
	if len(offsets) != resumableReaderMaxRetries {
		t.Fatalf("reopen calls=%d offsets=%v, want exactly %d", len(offsets), offsets, resumableReaderMaxRetries)
	}

	before := len(offsets)
	_, stickyErr := r.Read(make([]byte, 8))
	if stickyErr == nil || stickyErr.Error() != err.Error() {
		t.Fatalf("terminal error was not sticky: first=%v second=%v", err, stickyErr)
	}
	if after := len(opener.offsetsSnapshot()); after != before {
		t.Fatalf("terminal reader reopened again: calls %d -> %d", before, after)
	}
}

func TestActivity160599ResumableReaderRejectsNoProgressAndSizeOverrun(t *testing.T) {
	t.Run("no-progress", func(t *testing.T) {
		stalled := &activity160599NoProgressReader{failAfter: 100}
		r := NewResumableReader(
			context.Background(), &activity160599Opener{data: []byte("unused")}, 0, 1, 1, 8,
			stalled, slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
		_, err := io.ReadAll(r)
		if !errors.Is(err, io.ErrNoProgress) {
			t.Fatalf("error=%v, want io.ErrNoProgress", err)
		}
		if stalled.calls > resumableReaderMaxRetries+1 {
			t.Fatalf("reader accepted %d consecutive zero-progress reads", stalled.calls)
		}
	})

	t.Run("advertised-size-overrun", func(t *testing.T) {
		r := NewResumableReader(
			context.Background(), &activity160599Opener{}, 0, 1, 1, 3,
			io.NopCloser(bytes.NewReader([]byte("abcdef"))), slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
		if _, err := io.ReadAll(r); err == nil || !strings.Contains(err.Error(), "advertised size") {
			t.Fatalf("error=%v, want advertised-size overrun", err)
		}
	})
}

type activity160599FaultReader struct {
	data        []byte
	cut         int
	pos         int
	done        bool
	terminalErr error
}

func (r *activity160599FaultReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.pos >= r.cut {
		r.done = true
		return 0, r.terminalErr
	}
	n := len(p)
	if remaining := r.cut - r.pos; n > remaining {
		n = remaining
	}
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	if r.pos == r.cut {
		r.done = true
		return n, r.terminalErr
	}
	return n, nil
}

func (r *activity160599FaultReader) Close() error { return nil }

type activity160599NoProgressReader struct {
	calls     int
	failAfter int
}

func (r *activity160599NoProgressReader) Read([]byte) (int, error) {
	r.calls++
	if r.calls > r.failAfter {
		return 0, io.ErrNoProgress
	}
	return 0, nil
}
func (*activity160599NoProgressReader) Close() error { return nil }

type activity160599OneByteFaultOpener struct {
	mu      sync.Mutex
	data    []byte
	offsets []int64
}

func (o *activity160599OneByteFaultOpener) OpenLTXFile(_ context.Context, _ int, _, _ ltx.TXID, offset, _ int64) (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.offsets = append(o.offsets, offset)
	if offset >= int64(len(o.data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return &activity160599FaultReader{data: o.data[offset:], cut: 1, terminalErr: io.ErrUnexpectedEOF}, nil
}

func (o *activity160599OneByteFaultOpener) offsetsSnapshot() []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int64(nil), o.offsets...)
}

type activity160599Opener struct {
	mu      sync.Mutex
	data    []byte
	err     error
	offsets []int64
}

func (o *activity160599Opener) OpenLTXFile(_ context.Context, _ int, _, _ ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.offsets = append(o.offsets, offset)
	if o.err != nil {
		return nil, o.err
	}
	if offset < 0 || offset > int64(len(o.data)) {
		return nil, io.ErrUnexpectedEOF
	}
	end := int64(len(o.data))
	if size > 0 && offset+size < end {
		end = offset + size
	}
	return io.NopCloser(bytes.NewReader(o.data[offset:end])), nil
}

func (o *activity160599Opener) offsetsSnapshot() []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int64(nil), o.offsets...)
}
