package hashdb

import (
	"io"
	"os"
)

type (
	bytesReader struct {
		offset int
		buffer []byte
	}
)

func (reader *bytesReader) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	n := copy(data, reader.buffer[reader.offset:])
	reader.offset += n
	if n < len(data) {
		return n, io.EOF
	}

	return n, nil
}

func (reader *bytesReader) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	if len(data) == 0 {
		return 0, nil
	}
	if offset >= int64(len(reader.buffer)) {
		return 0, io.EOF
	}

	n := copy(data, reader.buffer[offset:])
	if n < len(data) {
		return n, io.EOF
	}

	return n, nil
}
