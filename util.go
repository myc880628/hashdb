package hashdb

import (
	"fmt"
	"os"
	"reflect"
	"unsafe"
)

func assert(condition bool, format string, args ...interface{}) {
	if !condition {
		fmt.Fprintf(os.Stderr, "assertion failed: "+format, args...)
		os.Exit(1)
	}
}

func byteSliceToString(b []byte) (s string) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh.Data = bh.Data
	sh.Len = bh.Len
	return
}
