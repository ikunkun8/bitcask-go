package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

//原子批量写数据，保证原子性

type WriteBatch struct {
	Options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

// NewWriteBatch
//
//	@Description: 初始化WriteBatch
//	@receiver db
//	@return *WriteBatch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch ,seq no file not exists")
	}
	return &WriteBatch{
		Options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存LogRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存LogRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.Options.MaxBatchSize {
		return ErrExceedMaxBatchNum
	}

	// 获取当前最新得事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	//开始写数据到文件数据中
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}
		positions[string(logRecord.Key)] = logRecordPos
	}

	// 标识事务完成得数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置决定是否持久化
	if wb.db.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	for _, logRecord := range wb.pendingWrites {
		pos := positions[string(logRecord.Key)]
		if logRecord.Type == data.LogRecordNormal {
			wb.db.index.Put(logRecord.Key, pos)
		}
		if logRecord.Type == data.LogRecordDeleted {
			wb.db.index.Delete(logRecord.Key)
		}
	}

	//清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key+Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], key[:n])
	copy(encKey[n:], key)

	return encKey
}

func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
