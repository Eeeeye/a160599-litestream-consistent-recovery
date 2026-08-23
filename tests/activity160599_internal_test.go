package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
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

	for _, cut := range []int{1, 31, 511, 4096, 8193, len(payload) - 1} {
		t.Run(ltx.TXID(cut).String(), func(t *testing.T) {
			opener := &activity160599Opener{data: payload}
			r := NewResumableReader(
				context.Background(), opener, 2, 11, 17, int64(len(payload)),
				&activity160599FaultReader{data: payload, cut: cut},
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

func TestActivity160599ResumableReaderHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opener := &activity160599Opener{data: []byte("abcdef"), err: context.Canceled}
	r := NewResumableReader(
		ctx, opener, 0, 1, 1, 6,
		&activity160599FaultReader{data: []byte("abcdef"), cut: 0},
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

type activity160599FaultReader struct {
	data []byte
	cut  int
	pos  int
	done bool
}

func (r *activity160599FaultReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.pos >= r.cut {
		r.done = true
		return 0, io.ErrUnexpectedEOF
	}
	n := len(p)
	if remaining := r.cut - r.pos; n > remaining {
		n = remaining
	}
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	if r.pos == r.cut {
		r.done = true
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func (r *activity160599FaultReader) Close() error { return nil }

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
