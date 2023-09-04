package fastcdc_test

import (
	"bytes"
	"math"
	"os"
	"testing"

	"codeberg.org/mhofmann/fastcdc"
)

// Length of each chunk in the test data file, as reported by a C implementation
// using the reference parameters.
var refLengths []int = []int{
	16890, 6013, 3376, 9855, 8253, 11070, 9310, 13159, 10051, 10067, 8318, 8503, 10589, 5419,
}

func TestRefChunker(t *testing.T) {
	f, err := os.Open("testdata/testfile.bin")
	if err != nil {
		t.Skip("no test data found")
		return
	}
	defer f.Close()

	chunker := fastcdc.NewRefChunker(f)

	for nchunk := 0; chunker.Next(); nchunk++ {
		chunk := chunker.Chunk()

		if nchunk >= len(refLengths) || len(chunk) != refLengths[nchunk] {
			t.Errorf("Unexpected chunk with length %d", len(chunk))
		}
	}

	if err = chunker.Err(); err != nil {
		t.Error("chunker.Err:", err)
	}
}

func TestCustomChunker(t *testing.T) {
	f, err := os.Open("testdata/testfile.bin")
	if err != nil {
		t.Skip("no test data found")
		return
	}
	defer f.Close()

	fs, err := f.Stat()
	if err != nil {
		t.Error("cannot stat testfile.bin:", err)
		return
	}

	minSizes := []int{1, 512, 1024, 2048, 4096, 4096, 4096}
	avgSizes := []int{16, 1024, 2048, 4096, 8192, 32768, 65536}
	maxSizes := []int{128, 4096, 8192, 16384, 32768, 81920, 131072}

	for i := 0; i < len(minSizes); i++ {
		_, err = f.Seek(0, 0)
		if err != nil {
			t.Error("Seek:", err)
			return
		}

		chunker, err := fastcdc.NewChunker(f, minSizes[i], avgSizes[i], maxSizes[i])
		if err != nil {
			t.Error("NewChunker:", err)
			return
		}

		var totalsize int64

		for chunker.Next() {
			totalsize += int64(len(chunker.Chunk()))
		}

		if err = chunker.Err(); err != nil {
			t.Error("chunker.Err:", err)
			return
		}

		if totalsize != fs.Size() {
			t.Errorf("Expected %d bytes in chunks, got %d", fs.Size(), totalsize)
		}
	}

}

func TestInvalidMinSize(t *testing.T) {
	var (
		b                bytes.Buffer
		avgSize, maxSize int
		minSizes         []int
	)

	avgSize = 8192
	maxSize = 65536

	minSizes = []int{0, -1, avgSize + 1}

	for _, minSize := range minSizes {
		_, err := fastcdc.NewChunker(&b, minSize, avgSize, maxSize)
		if err != fastcdc.ErrParam {
			t.Errorf("Expected ErrParam for minSize %d, got %v", minSize, err)
		}
	}
}

func TestInvalidAvgSize(t *testing.T) {
	var (
		b                bytes.Buffer
		minSize, maxSize int
		avgSizes         []int
	)

	minSize = 2048
	maxSize = math.MaxInt

	avgSizes = []int{0, 2, -2, minSize / 2, maxSize}

	for _, avgSize := range avgSizes {
		_, err := fastcdc.NewChunker(&b, minSize, avgSize, maxSize)
		if err != fastcdc.ErrParam {
			t.Errorf("Expected ErrParam for avgSize %d, got %v", avgSize, err)
		}
	}
}

func TestInvalidMaxSize(t *testing.T) {
	var (
		b                bytes.Buffer
		minSize, avgSize int
		maxSizes         []int
	)

	minSize = 2048
	avgSize = 65536

	maxSizes = []int{0, -1, minSize - 1, avgSize - 1}

	for _, maxSize := range maxSizes {
		_, err := fastcdc.NewChunker(&b, minSize, avgSize, maxSize)
		if err != fastcdc.ErrParam {
			t.Errorf("Expected ErrParam for maxnSize %d, got %v", maxSize, err)
		}
	}
}
