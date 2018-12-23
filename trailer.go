package hashdb

import (
	"encoding/binary"
	"io"
)

type (
	trailer struct {
		sum32 uint32
	}
)

const (
	trailerSize = int(unsafe.Sizeof(trailer{}))
)

func (t *trailer) encode(writer io.Writer) {
	binary.Write(writer, binary.BigEndian, t.sum32)
}

func (t *trailer) decode(reader io.Reader) {
	binary.Read(reader, binary.BigEndian, &t.sum32)
}
