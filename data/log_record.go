package data

import (
	"encoding/binary"
	"hash/crc32"
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
// +-----------+------------+-------------+--------------+-----------+---------------+
// / crc 校验值 /  type 类型  /  key size   /  value size  /    key    /     value     /
// +-----------+------------+-------------+--------------+-----------+---------------+
//
//	4字节 		 1字节	     变长（最大5）	 变长（最大5）       变长			变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	//第五个字节存储Type
	header[4] = logRecord.Type
	var index = 5

	//第五个字节之后存储key和value的长度信息
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	//拷贝header的内容
	copy(encBytes[:index], header[:index])

	//拷贝key和value数据到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个LogRecord的数据进行crc校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader
//
//	@Description: 对字节数组中的Header信息进行解码
//	@param buf 从磁盘中读取到的LogRecord的头部信息字节数组
//	@return *logRecordHeader
//	@return int64 header的实际大小。包括crc+type+keysize+valuesize
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {

	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5

	// 取出实际的key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
