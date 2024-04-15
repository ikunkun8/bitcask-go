package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int //只能用于加载索引时使用的文件id
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index      index.Indexer //内存索引
}

// Open
//
//	@Description: 打开 bitcask 存储引擎实例
//	@param options
//	@return *DB
//	@return error
func Open(options Options) (*DB, error) {
	//  对传入的配置进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//判断数据目录是否存在，不存在则创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化Db实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}
	//  从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// Put 写入key、value
func (db *DB) Put(key []byte, value []byte) error {
	// key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入数据到当前活跃数据文件当中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	// 判断key的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查key是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos != nil {
		return nil
	}

	//	构造 LogRecord，标识是被删除的key
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	// 写入到数据文件中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	//从内存索引中将对应的Key删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//  从内存数据结构中取出key对应得索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//  根据偏移读取对应得数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type != data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	db.mu.Lock()
	defer db.mu.Unlock()

	//判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的  如果为空则初始化数据文件
	if db.activeFile != nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	//如果写入的数据已经达到了活跃文件的阈值，关闭活跃文件，并打开新的
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//持久化数据，保证已有的数据持久到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//根据用户配置决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

// 设置当前活跃文件 在访问此方法之前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	//打开数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {

	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			//  数据目录有可能被损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//  对文件id进行排序，从小到大依次加载
	sort.Ints(fileIds)

	//  遍历每个文件id，打开对应的数据文件

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 最后一个是活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 从数据文件加载索引
//
//	@Description:
//	@receiver db
//	@return error
func (db *DB) loadIndexFromDataFiles() error {

	//没有文件说明数据库是空的
	if len(db.fileIds) == 0 {
		return nil
	}

	//遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0

		//循环处理文件中的内容
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//构造内存索引
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			//递增offset,下一次从新的位置读取
			offset += size
		}

		//如果当前是活跃文件，记录文件的WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("DirPath is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("DataFileSize must be greater than 0")
	}
	return nil
}
