package data

import (
	"encoding/binary"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录，之所以叫日志是应为是以追加写的方式写入的，类似日志
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// LogRecordPos 记录了LogRecord的位置，他是放在磁盘上的
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组以及长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 对字节数组中的Header信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lc *LogRecord, header []byte) uint32 {
	return 0
}
