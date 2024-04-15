package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

// DataFile
// @Description: 磁盘上的数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

// Write
//
//	@Description:
//	@receiver df
//	@param buf
//	@return error
func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
