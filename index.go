package hashdb

import (
	"sync"
	"unsafe"
)

type (
	recordIndex struct {
		header       // 8 byte
		offset int64 // 8 byte
	}
)

const (
	recordIndexSize = int(unsafe.Sizeof(recordIndex{}))
)

type (
	indexTable struct {
		mu    sync.RWMutex
		usage int // memory usage of all items
		table map[string]*recordIndex
	}
)

func newIndexTable() *indexTable {
	return &indexTable{
		table: make(map[string]*recordIndex, 1024),
	}
}

func (it *indexTable) memoryUsage() (usage int) {
	it.mu.RLock()
	usage = it.usage
	it.mu.RUnlock()
	return
}

func (it *indexTable) size() (sz int) {
	it.mu.RLock()
	sz = len(it.table)
	it.mu.RUnlock()
	return
}

func (it *indexTable) put(key []byte, newIndex *recordIndex) (oldIndex *recordIndex) {
	assert(key != nil, "the key of new index is not empty")

	it.mu.Lock()
	oldIndex = it.tryRemove(key)

	it.table[string(key)] = newIndex
	it.usage += len(key) + recordIndexSize
	it.mu.Unlock()
	return
}

func (it *indexTable) get(key []byte) (index *recordIndex) {
	it.mu.RLock()
	index = it.table[string(key)]
	it.mu.RUnlock()
	return
}

func (it *indexTable) tryRemove(key []byte) (index *recordIndex) {
	if index = it.table[string(key)]; index != nil {
		delete(it.table, byteSliceToString(key))
		it.usage -= len(key) + recordIndexSize
	}
	return index
}

func (it *indexTable) remove(key []byte) (index *recordIndex) {
	it.mu.Lock()
	index = it.tryRemove(key)
	it.mu.Unlock()
	return
}

func (it *indexTable) clear() {
	it.mu.Lock()
	it.table = make(map[string]*recordIndex)
	it.usage = 0
	it.mu.Unlock()
}
