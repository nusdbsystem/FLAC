package log

import "go.uber.org/zap/buffer"

type Manager struct {
	bufferSiz uint32
	buffer.Buffer
}
