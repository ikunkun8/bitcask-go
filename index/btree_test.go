package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {

	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

}

func TestBtree_Get(t *testing.T) {

	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)

}

func TestBtree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)
	res5 := bt.Get([]byte("aaa"))
	t.Log(res5)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
	res6 := bt.Get([]byte("aaa"))
	t.Log(res6)
}

func TestBTree_Iterator(t *testing.T) {
	bt := NewBTree()
	// 1.BTree 为空的情况
	iter1 := bt.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2.BTree 有数据的情况
	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 31, Offset: 31})
	iter2 := bt.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.BTree 有多条数据的输出
	bt.Put([]byte("acee"), &data.LogRecordPos{Fid: 31, Offset: 31})
	bt.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 33, Offset: 3221})
	bt.Put([]byte("ccde"), &data.LogRecordPos{Fid: 34, Offset: 33})
	bt.Put([]byte("eede"), &data.LogRecordPos{Fid: 34, Offset: 33})
	iter3 := bt.Iterator(false)
	assert.Equal(t, true, iter3.Valid())
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log("key=", string(iter3.Key()))
	}

	iter4 := bt.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//t.Log("key=", string(iter4.Key()))
	}

	// 4.测试 seek
	iter5 := bt.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		t.Log(string(iter5.Key()))
	}

	// 5.反向遍历的 seek
	iter6 := bt.Iterator(true)
	for iter6.Seek([]byte("ccf")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}

}
