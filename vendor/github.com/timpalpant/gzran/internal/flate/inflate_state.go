package flate

type huffmanTable int

const (
	nilHuffmanTable huffmanTable = iota
	fixedHuffmanTable
	dynamicHuffmanTable
)

// readerState is used to serialized the current decompressor state.
type readerState struct {
	Roffset int64

	// Input bits, in top of B.
	B  uint32
	Nb uint

	// Huffman decoders for literal/length, distance.
	H1, H2 huffmanDecoder

	// Length arrays used to define Huffman codes.
	Bits     *[maxNumLit + maxNumDist]int
	Codebits *[numCodes]int

	// Output history, buffer.
	Dict dictDecoder

	// Next Step in the decompression,
	// and decompression state.
	Step         inflateStep
	StepState    int
	Final        bool
	ToRead       []byte
	HuffmanTable huffmanTable
	CopyLen      int
	CopyDist     int
}
