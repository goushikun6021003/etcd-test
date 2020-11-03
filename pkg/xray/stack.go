package xray

import (
	"log"
	"runtime"
)

func PrintStack() {
	stack := make([]byte, 1024*16)
	for {
		// get goroutine`s stacktrace, if need all second parameter is true
		size := runtime.Stack(stack, false)
		// the size of the buffer may be not enough to hold the stacktrace, so double the buffer size
		if size == len(stack) {
			stack = make([]byte, len(stack)<<1)
			continue
		}
		break
	}
	log.Printf("[Runtime Stack] %s", stack)
}
