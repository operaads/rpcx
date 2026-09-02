package protocol

import (
	"strconv"
	"testing"

	"github.com/smallnest/rpcx/util"
)

func newBenchmarkMessageWithPayload(payloadSize int) *Message {
	message := NewMessage()
	message.ServicePath = "Forecast"
	message.ServiceMethod = "Predict"
	message.Metadata = map[string]string{
		"__ID": "benchmark-request",
	}
	message.Payload = make([]byte, payloadSize)
	return message
}

func benchmarkMessageEncodeSlicePointer(b *testing.B, maxPoolSize, payloadSize int) {
	previousPool := bufferPool
	bufferPool = util.NewLimitedPool(512, maxPoolSize)
	defer func() {
		bufferPool = previousPool
	}()

	message := newBenchmarkMessageWithPayload(payloadSize)
	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := message.EncodeSlicePointer()
		PutData(data)
	}
}

func BenchmarkMessageEncodeSlicePointer(b *testing.B) {
	for _, payloadSize := range []int{1024, 4096, 16384, 65536, 131072, 262144} {
		b.Run(strconv.Itoa(payloadSize), func(b *testing.B) {
			b.Run("pool-4KB", func(b *testing.B) {
				benchmarkMessageEncodeSlicePointer(b, 4096, payloadSize)
			})
			b.Run("pool-256KB", func(b *testing.B) {
				benchmarkMessageEncodeSlicePointer(b, 262144, payloadSize)
			})
		})
	}
}
