package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// BenchmarkExecuteHandler measures dispatch + handler overhead per task type.
func BenchmarkExecuteHandler(b *testing.B) {
	mux := NewMux()
	mux.Handle("resize_image", func(_ context.Context, task Task) error { return nil })
	mux.Handle("scrape_url", func(_ context.Context, task Task) error { return nil })
	mux.Handle("generate_report", func(_ context.Context, task Task) error { return nil })

	cases := []Task{
		{Type: "resize_image", Payload: json.RawMessage(`{"image_url":"https://picsum.photos/300","width":600,"height":400}`)},
		{Type: "scrape_url", Payload: json.RawMessage(`{"url":"https://example.com"}`)},
		{Type: "generate_report", Payload: json.RawMessage(`{"date":"2026-01-01"}`)},
	}

	ctx := context.Background()
	for _, tc := range cases {
		b.Run(tc.Type, func(b *testing.B) {
			b.ReportAllocs()
			h, _ := mux.handler(tc.Type)
			for i := 0; i < b.N; i++ {
				h(ctx, tc)
			}
		})
	}
}

// BenchmarkProcessWithRetry measures the full single-task pipeline: fetch →
// cancelled check → handler dispatch → status update → attempt log.
func BenchmarkProcessWithRetry(b *testing.B) {
	store := newFakeStore()
	mux := NewMux()
	mux.Handle("scrape_url", func(_ context.Context, task Task) error { return nil })

	ids := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("bench-%d", i)
		ids[i] = id
		store.put(Task{ID: id, Type: "scrape_url", Payload: json.RawMessage(`{"url":"https://example.com"}`), Priority: "medium"})
	}

	pool := NewPool(&fakeQueue{}, store, mux, 1, testLogger())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pool.processWithRetry(ids[i], 0, "")
	}
}
