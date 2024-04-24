package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path)

	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.NotNil(t, res1)

	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res2)
	res3 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 13})
	assert.NotNil(t, res3)

	res4 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 15})
	assert.NotNil(t, res4)

}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 11})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 15})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)

}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path)

	res1 := tree.Delete([]byte("not exist"))
	assert.False(t, res1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	res2 := tree.Delete([]byte("aac"))
	assert.True(t, res2)

	pos2 := tree.Get([]byte("aac"))
	assert.Nil(t, pos2)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	t.Log(path)
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path)

	assert.Equal(t, 0, tree.Size())
	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.NotNil(t, res1)

	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res2)
	res3 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 1, Offset: 13})
	assert.NotNil(t, res3)

	assert.Equal(t, 3, tree.Size())

}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	t.Log(path)
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path)

	tree.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Value())
		assert.NotNil(t, iter.Key())
	}
}
