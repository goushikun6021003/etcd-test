package xray

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
)

// XErr basic
type XErr struct {
	Msg      string    // 对应 Error()
	rawErr   error     // 初始错误信息 不会被更新
	rawStack []uintptr // 初始错误的调用栈
}

type LogErr struct {
}

func (e *XErr) Error() string {
	return e.Msg
}

// RawErr the origin err
func (e XErr) RawErr() error {
	return e.rawErr
}

// RawStackFull get function call stack
func (e XErr) RawStackFull() string {
	frames := runtime.CallersFrames(e.rawStack)
	var (
		frame  runtime.Frame
		more   bool
		result string
		index  int
	)
	for {
		frame, more = frames.Next()
		if index = strings.Index(frame.File, "src"); index != -1 {
			// trim GOPATH or GOROOT prifix
			frame.File = string(frame.File[index+4:])
		}
		result = fmt.Sprintf("%s%s\n\t%s:%d\n", result, frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
	return result
}

// RawStackMini get function call stack
func (e XErr) RawStackMini() string {
	pc, _, _, _ := runtime.Caller(2)
	file, line := runtime.FuncForPC(pc).FileLine(pc)
	funcFileLine := fmt.Sprintf("%s:%d", file, line)
	funcNameSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	result := fmt.Sprintf("%s %s", funcFileLine, funcNameSlice[len(funcNameSlice)-1])
	return result
}

// ErrDetail get detail error info
func ErrDetail(err error) {
	e := Wrap(err, 0)
	log.Printf("err msg: %s\nerr raw: %s\nerr stack: %s\n",
		e.Error(),
		e.RawErr(),
		e.RawStackFull(),
	)
}

// ErrMini get mini error info
func ErrMini(err error) {
	e := Wrap(err, 0)
	log.Printf("%s\n|------------------ %s\n",
		e.Error(),
		e.RawStackMini(),
	)
}

// combine raw error info and rawStack error info in the base error and return as a new error
func ErrMiniInfo(err error) error {
	e := Wrap(err, 0)
	newErr := errors.New(fmt.Sprintf("%s\n|------------------ %s\n", e.Error(), e.RawStackMini()))
	return newErr
}

func ErrTaskInfo(err error, taskID, jobID string) {
	log.Printf("[TaskID]: %s\n[JobID]: %s\n[ErrInfo]: %s", taskID, jobID, err)
}

// Wrap notice: be careful, the returned value is *MErr, not error
func Wrap(err error, fmtAndArgs ...interface{}) *XErr {
	return WrapDepth(1, err, fmtAndArgs...)
}

// WrapDepth The argument depth is the number of stack frames to skip before recording in pc, with 0 identifying the caller of WrapDepth.
// if a wrapper is added to WrapDepth, depth should +1, like Wrap.
func WrapDepth(depth int, err error, fmtAndArgs ...interface{}) *XErr {
	msg := fmtErrMsg(fmtAndArgs...)
	if err == nil {
		err = errors.New(msg)
	}
	if e, ok := err.(*XErr); ok {
		if msg != "" {
			e.Msg = msg
		}
		return e
	}
	pcs := make([]uintptr, 32)
	// skip some first invocations
	count := runtime.Callers(2+depth, pcs)
	e := &XErr{
		Msg:      msg,
		rawErr:   err,
		rawStack: pcs[:count],
	}
	if e.Msg == "" {
		e.Msg = err.Error()
	}
	return e
}

// fmtErrMsg used to format error message
func fmtErrMsg(msgs ...interface{}) string {
	if len(msgs) > 1 {
		return fmt.Sprintf(msgs[0].(string), msgs[1:]...)
	}
	if len(msgs) == 1 {
		if v, ok := msgs[0].(string); ok {
			return v
		}
		if v, ok := msgs[0].(error); ok {
			return v.Error()
		}
	}
	return ""
}
