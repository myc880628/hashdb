package hashdb

import (
	"hash/crc32"
	"io"
	"unsafe"
	"errors"
	"bytes"
	
	"github.com/golang/snappy"
)

var (
	ErrBadKeySize   = errors.New("hashdb/record: bad key size")
	ErrBadValueSize = errors.New("hashdb/record: bad value size")
	ErrBadChecksum  = errors.New("hashdb/record: bad checksum")
)

const (
	kb = 1024
	mb = 1024 * kb
	//gb = 1024 * mb
)

const (
	maxKeySize   = 1 * kb
	maxValueSize = 8 * mb
)

const (
	typeValue    = 0x00
	typeDeletion = 0x01
)

const (
	compressFlag       = 0x02
	compressRatioLimit = 0.70
)

const (
	paddingSize = 256
)

//                    2       2       4      ksize       vsize        4
// record format: | flags | ksize | vsize | key data | value data | sum32  |
//                | ---------header------ |    key   |     value  | trailer|
type (
	record struct {
		header
		key   []byte
		value []byte
	}
)

func newRecord(key, value []byte, args ...bool) *record {
	r := &record{}

	r.key = key
	r.ksize = uint16(len(key))

	r.value = value
	r.vsize = uint32(len(value))

	// is delete operation
	if len(args) > 0 && args[0] {
		assert(len(value) == 0, "the value of the delete operation should be empty")
		r.flags |= typeDeletion
	} else {
		// default is set kv operation
		r.flags |= typeValue
	}

	return r
}

func (r *record) isDeleted() bool {
	return r.flags&typeDeletion != 0
}

func (r *record) isValueCompressed() bool {
	return r.flags&compressFlag != 0
}

// disk size for this record
func (r *record) size() int {
	return int(r.ksize) + int(r.vsize) + headerSize + trailerSize
}

func (r *record) tryCompressValue() (ok bool) {
	assert(r.flags&compressFlag == 0, "the record has not been compressed yet")
	if r.size() > paddingSize {
		value := snappy.Encode(nil, r.value)
		if len(value) <= int(float64(r.vsize)*compressRatioLimit) {
			ok = true
			r.value = value
			r.vsize = uint32(len(value))
			r.flags |= compressFlag
		}
	}
	return
}

func (r *record) tryDecompressValue() {
	if r.flags&compressFlag != 0 {
		value, _ := snappy.Decode(nil, r.value)
		r.value = value
		r.vsize = uint32(len(value))
		r.flags &^= compressFlag
	}
}

func (r *record) encode(writer io.Writer) error {
	hash := crc32.NewIEEE()
	multiWriter := io.MultiWriter(writer, hash)

	// serialize header
	r.header.encode(multiWriter)
	// serialize key
	multiWriter.Write(r.key)
	// serialize value
	multiWriter.Write(r.value)
	// serialize trailer
	trailer{hash.Sum32()}.encode(writer)

	return nil
}

func (r *record) checksum() uint32 {
	hash := crc32.NewIEEE()

	r.header.encode(hash)
	hash.Write(r.key)
	hash.Write(r.value)

	return hash.Sum32()
}

func (r *record) decode(offset int64, reader io.ReaderAt) error {
	// read value & trailer(crc32)
	buffer := make([]byte, int(r.vsize)+trailerSize)
	_, err := reader.ReadAt(buffer, offset+int64(headerSize)+int64(r.ksize))
	if err != nil {
		return err
	}

	r.value = buffer[:r.vsize:r.vsize]

	trailer := trailer{}
	trailer.decode(bytes.NewReader(buffer[r.vsize:]))
	if r.checksum() != trailer.sum32 {
		return ErrBadChecksum
	}

	return nil
}
