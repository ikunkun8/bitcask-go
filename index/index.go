package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象的索引接口，如果后续需要接入其他的数据结构，直接实现这个接口就可以了
// 在内存中，根据key找到存放的日志的位置信息
type Indexer interface {
	// Put 向索引中添加key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 得到key对应的数据位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 删除key对应的数据位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("invalid indexer")
	}
}

// Item 在内存中表示一个键值对，主要是为了给btree使用，google的btree库必须这个条目
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
