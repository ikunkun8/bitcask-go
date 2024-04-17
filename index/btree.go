package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"sync"
)

// BTree 在内存中索引的数据结构。

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 初始化BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}

	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true

}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

//// BTree 索引迭代器
//type btreeIterator struct {
//	currIndex int     //当前位置
//	reverse   bool    //是否是反向遍历
//	values    []*Item //key+位置索引信息
//}
//
//func (bti *btreeIterator) Rewind() {
//
//}
//
//func (bti *btreeIterator) Seek(key []byte) {
//
//}
//
//func (bti *btreeIterator) Next() {
//
//}
//
//func (bti *btreeIterator) Valid() bool {
//
//}
//
//func (bti *btreeIterator) Key() []byte {
//
//}
//
//func (bti *btreeIterator) Value() *data.LogRecordPos {
//
//}
//
//func (bti *btreeIterator) Close() {
//
//}
