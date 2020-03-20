package main

import (
	"fmt"
	"os"
	"sort"
)

type (
	Key interface {
		Compare(key Key) int
	}
)

type (
	node struct {
		root    bool
		leaf    bool
		pgNo    int64
		parent  *node
		keys    []Key
		offsets []int64
	}
)

type (
	Integer int
)

func (i Integer) Compare(k Key) int {
	j, ok := k.(Integer)
	if !ok {
		panic("Integer.Compare: invalid key")
	}
	return int(i - j)
}

func (n *node) insert(i int, key Key, offset int64) {
	n.keys = append(n.keys, nil)
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key
	// insert offset
	n.offsets = append(n.offsets, 0)
	copy(n.offsets[i+1:], n.offsets[i:])
	n.offsets[i] = offset
}

func (n *node) delete(i int) {
	if i < len(n.keys) {
		copy(n.keys[i:], n.keys[i+1:])
	}
	n.keys = n.keys[:len(n.keys)-1]
	// delete offset
	copy(n.offsets[i:], n.offsets[i+1:])
	n.offsets = n.offsets[:len(n.offsets)-1]
}

func (n *node) numKeys() int {
	return len(n.keys)
}

func (n *node) rem(key Key) (old int64, ex bool) {
	assert(n.leaf, "node.rem: must be a leaf node")
	i, ex := n.search(key)
	if ex {
		old = n.offsets[i]
		// delete child
		n.delete(i)
	}
	return
}

func (n *node) put(key Key, offset int64) (old int64, ex bool) {
	assert(n.leaf, "node.put: must be a leaf node")
	i, ex := n.search(key)
	if ex {
		old = n.offsets[i]
		// replace
		n.offsets[i] = offset
	} else {
		n.insert(i, key, offset)
	}
	return
}

func (n *node) search(key Key) (i int, ex bool) {
	i = sort.Search(len(n.keys), func(j int) bool {
		switch v := key.Compare(n.keys[j]); {
		case v < 0:
			return true
		case v == 0:
			ex = true
			return true
		default:
			return false
		}
	})
	return
}

func assert(condition bool, format string, args ...interface{}) {
	if !condition {
		fmt.Fprintf(os.Stderr, "assertion failed: "+format, args...)
		os.Exit(1)
	}
}

func main() {
	n := node{}
	n.leaf = true
	n.put(Integer(1), 1)
	n.put(Integer(-1), -1)
	n.put(Integer(1), 3)
	fmt.Println(n.rem(Integer(1)))
	fmt.Println(n.numKeys())
	//fmt.Printf("%+v\n", n)
}
