package xray

import (
	"testing"
)

// 测试err报错
type TE struct {
}

func (t TE) Error() string {
	return "This is testing error!!!"
}

func TestERRLog(t *testing.T) {
	err := TE{}
	ErrMini(err)
}
