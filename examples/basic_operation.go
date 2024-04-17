package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "I:\\acgit project\\note-in-loongson\\kv-projects\\tmp"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val1, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val1 = ", string(val1))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

}
