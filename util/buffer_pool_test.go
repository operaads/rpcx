package util

import (
	"fmt"
	"testing"
)

func TestLimitedPool_findPool(t *testing.T) {
	pool := NewLimitedPool(512, 131072)

	tests := []struct {
		name string
		args int
		want int
	}{
		{name: "below minimum", args: 200, want: 512},
		{name: "minimum", args: 512, want: 512},
		{name: "between levels", args: 1000, want: 1024},
		{name: "next level", args: 2000, want: 2048},
		{name: "exact level", args: 2048, want: 2048},
		{name: "just below maximum", args: 131071, want: 131072},
		{name: "maximum", args: 131072, want: 131072},
		{name: "above maximum", args: 131073, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pool.findPool(tt.args)
			if tt.want < 0 {
				if got != nil {
					t.Fatalf("findPool(%d) = %d, want nil", tt.args, got.size)
				}
				return
			}
			if got == nil {
				t.Fatalf("findPool(%d) = nil, want %d", tt.args, tt.want)
			}
			if got.size != tt.want {
				t.Errorf("findPool(%d) = %d, want %d", tt.args, got.size, tt.want)
			}
		})
	}
}

func TestLimitedPool_findPutPool(t *testing.T) {
	pool := NewLimitedPool(512, 131072)

	tests := []struct {
		name string
		args int
		want int
	}{
		{name: "below minimum", args: 200, want: -1},
		{name: "minimum", args: 512, want: 512},
		{name: "between levels", args: 1000, want: 512},
		{name: "next level", args: 2000, want: 1024},
		{name: "exact level", args: 2048, want: 2048},
		{name: "just below maximum", args: 131071, want: 65536},
		{name: "maximum", args: 131072, want: 131072},
		{name: "above maximum", args: 131073, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pool.findPutPool(tt.args)
			if tt.want < 0 {
				if got != nil {
					t.Fatalf("findPutPool(%d) = %d, want nil", tt.args, got.size)
				}
				return
			}
			if got == nil {
				t.Fatalf("findPutPool(%d) = nil, want %d", tt.args, tt.want)
			}
			if got.size != tt.want {
				t.Errorf("findPutPool(%d) = %d, want %d", tt.args, got.size, tt.want)
			}
		})
	}
}

func TestLimitedPool_levels(t *testing.T) {
	pool := NewLimitedPool(512, 131072)
	want := []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072}

	if len(pool.pools) != len(want) {
		t.Fatalf("got %d pool levels, want %d", len(pool.pools), len(want))
	}
	for i, size := range want {
		if pool.pools[i].size != size {
			t.Errorf("pool level %d = %d, want %d", i, pool.pools[i].size, size)
		}
	}
}

func TestLimitedPool_Get(t *testing.T) {
	pool := NewLimitedPool(512, 131072)

	tests := []struct {
		name    string
		size    int
		wantCap int
	}{
		{name: "minimum", size: 512, wantCap: 512},
		{name: "between levels", size: 4097, wantCap: 8192},
		{name: "maximum", size: 131072, wantCap: 131072},
		{name: "above maximum", size: 131073, wantCap: 131073},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := pool.Get(tt.size)
			defer pool.Put(buf)

			if len(*buf) != tt.size {
				t.Errorf("Get(%d) length = %d, want %d", tt.size, len(*buf), tt.size)
			}
			if cap(*buf) != tt.wantCap {
				t.Errorf("Get(%d) capacity = %d, want %d", tt.size, cap(*buf), tt.wantCap)
			}
		})
	}
}

func TestLimitedPool_invalidSize(t *testing.T) {
	tests := []struct {
		name    string
		minSize int
		maxSize int
	}{
		{name: "maximum below minimum", minSize: 1024, maxSize: 512},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("NewLimitedPool did not panic")
				}
			}()
			NewLimitedPool(tt.minSize, tt.maxSize)
		})
	}
}

func ExampleLimitedPool() {
	pool := NewLimitedPool(512, 131072)
	buf := pool.Get(4097)
	fmt.Println(len(*buf), cap(*buf))
	pool.Put(buf)
	// Output: 4097 8192
}
