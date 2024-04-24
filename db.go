package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIds         []int                     //只能用于加载索引时使用的文件id
	activeFile      *data.DataFile            //活跃文件
	olderFiles      map[uint32]*data.DataFile //旧文件，只能读
	index           index.Indexer             //内存索引
	seqNo           uint64                    //事务序列号，全局递增
	isMerging       bool                      //是否有文件在merge
	seqNoFileExists bool                      //是否已经存在seqnofile
	isInitial       bool                      //是否是第一次初始化此目录
}

const seqNoKey = "seq.no"

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
	var isInitial bool
	//判断数据目录是否存在，不存在则创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}
	//初始化Db实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
	}

	// 加载merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	if options.IndexType != BPlusTree {
		// 从hint索引文件加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		//  从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	} else {
		//取出当前事务序列号
		if err := db.loadSeqNO(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

// 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}
	//保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)

	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
	}
	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	//	关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put 写入key、value
func (db *DB) Put(key []byte, value []byte) error {
	// key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入数据到当前活跃数据文件当中
	pos, err := db.appendLogRecordWithLock(logRecord)
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
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//	构造 LogRecord，标识是被删除的key
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	// 写入到数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
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

	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取数据库所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作
func (db *DB) Fold(fn func(k []byte, v []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	//判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的  如果为空则初始化数据文件
	if db.activeFile == nil {
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
	db.fileIds = fileIds
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

	//查看是否发生过merge
	hasMerge, nonMergeFileID := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileID(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileID = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index")
		}
	}

	//暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo
	//遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//如果比最近未参与merge的文件id更小，则说明已经从hint中加载过了
		if hasMerge && fileId < nonMergeFileID {
			continue
		}
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

			//解析 Key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//事务完成，对应得seq no数据更新到内存索引当中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//递增offset,下一次从新的位置读取
			offset += size
		}

		//如果当前是活跃文件，记录文件的WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	//更新事务序列号
	db.seqNo = currentSeqNo
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

func (db *DB) loadSeqNO() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return nil
}
