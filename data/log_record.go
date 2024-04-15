package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录，之所以叫日志是应为是以追加写的方式写入的，类似日志
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 记录了LogRecord的位置，他是放在磁盘上的
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
