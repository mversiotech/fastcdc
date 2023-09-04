package fastcdc_test

import (
	"fmt"
	"os"

	"codeberg.org/mhofmann/fastcdc"
)

func Example() {
	f, err := os.Open("testdata/testfile.bin")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var nChunks, nBytes int

	chunker := fastcdc.NewRefChunker(f)

	for chunker.Next() {
		chunk := chunker.Chunk()
		nBytes += len(chunk)
		nChunks++
	}

	if err = chunker.Err(); err != nil {
		panic(err)
	}

	fmt.Printf("read %d chunks with %d bytes total\n", nChunks, nBytes)

	// Output:
	// read 14 chunks with 130873 bytes total
}
