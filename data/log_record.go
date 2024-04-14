package data

// LogRecordPos 记录了LogRecordPos的位置，他是放在磁盘上的
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}
