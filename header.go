package hashdb

import (
	"encoding/binary"
	"io"
)

type (
	header struct {
		flags uint16
		ksize uint16
		vsize uint32
	}
)

const (
	headerSize = int(unsafe.Sizeof(header{}))
)

func (h *header) encode(writer io.Writer) {
	binary.Write(writer, binary.BigEndian, h.flags)
	binary.Write(writer, binary.BigEndian, h.ksize)
	binary.Write(writer, binary.BigEndian, h.vsize)
}

func (h *header) decode(reader io.Reader) {
	binary.Read(reader, binary.BigEndian, &h.flags)
	binary.Read(reader, binary.BigEndian, &h.ksize)
	binary.Read(reader, binary.BigEndian, &h.vsize)
}
