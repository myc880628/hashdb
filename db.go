package hashdb

import (
	"bytes"
	"github.com/pkg/errors"
	"os"
	"sync"
)

type (
	HashDB struct {
		lock   sync.Mutex
		size   int64
		file   *os.File
		path   string
		buffer bytes.Buffer // write buffer
		iTable *indexTable
	}
)

var (
	ErrNotFound = errors.New("record not found")
)

func (db *HashDB) Path() string {
	return db.path
}

func (db *HashDB) put(key, value []byte) (err error) {
	assert(key != nil, "db.put: key is not empty")
	assert(value != nil, "db.put: value is not empty")

	r := newRecord(key, value)
	r.tryDecompressValue()

	fileSize := db.size
	if err = db.writeRecord(r); err != nil {
		return
	}

	index := &recordIndex{
		header: r.header,
		offset: fileSize,
	}
	db.iTable.put(copyBytes(key), index)

	return nil
}

func (db *HashDB) Put(key, value []byte) (err error) {
	if len(key) == 0 || len(key) > maxKeySize {
		return ErrBadKeySize
	}
	if len(value) > maxValueSize {
		return ErrBadValueSize
	}
	if value == nil {
		value = []byte{}
	}

	db.lock.Lock()
	err = db.put(key, value)
	db.lock.Unlock()

	return
}

func (db *HashDB) get(key []byte) (*record, error) {
	index := db.iTable.get(key)
	if index == nil {
		return nil, nil
	}
	r := record{
		key:    key,
		header: index.header,
	}
	err := r.decode(index.offset, db.file)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (db *HashDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 || len(key) > maxKeySize {
		return nil, ErrBadKeySize
	}

	r, err := db.get(key)
	if err != nil {
		return nil, err
	}
	if r == nil || r.isDeleted() {
		return nil, ErrNotFound
	}
	if r.isValueCompressed() {
		r.tryDecompressValue()
	}
	return r.value, nil
}

func (db *HashDB) writeRecord(r *record) error {
	db.buffer.Reset()
	_ = r.encode(&db.buffer)
	if size := db.buffer.Len(); size%paddingSize != 0 {
		// align to padding size
		db.buffer.Write(make([]byte, paddingSize-size%paddingSize))
	}

	n, err := db.file.Write(db.buffer.Bytes())
	if err != nil {
		_ = db.file.Truncate(db.size)
		return err
	}

	db.size += int64(n)
	return nil
}

func (db *HashDB) remove(key []byte) error {
	index := db.iTable.get(key)
	if index == nil {
		return nil
	}

	r := newRecord(key, []byte{}, true)
	if err := db.writeRecord(r); err != nil {
		return err
	}

	db.iTable.remove(key)
	return nil
}

func (db *HashDB) Remove(key []byte) (err error) {
	if len(key) == 0 || len(key) > maxKeySize {
		return ErrBadKeySize
	}

	db.lock.Lock()
	err = db.remove(key)
	db.lock.Unlock()

	return err
}
