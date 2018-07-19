package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	//_ "net/http/pprof"
	"os"
	"time"

	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"strings"
)

type Transaction struct {
	From  string
	To    string
	Value uint64
}

func NewTransaction(from string, to string, value uint64) *Transaction {
	return &Transaction{
		From:  from,
		To:    to,
		Value: value,
	}
}

func NewCoinBase() *Transaction {
	return &Transaction{
		From:  "",
		To:    "a",
		Value: 1000000000,
	}
}

type Block struct {
	Transactions []*Transaction
	Hash         uint64
}

type BlockChain struct {
	db *leveldb.DB
}

func NewBlockChain(file string) *BlockChain {
	chain := &BlockChain{}
	// default Options
	o := opt.Options{
		NoSync: false,
		//Filter: filter.NewBloomFilter(10),
	}

	db, err := leveldb.OpenFile(file, &o)

	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(file, nil)
	}

	chain.db = db

	return chain
}

func (self *BlockChain) Save(block *Block) {
	fmt.Println("begin process block")
	encoded := bytes.NewBuffer(nil)
	//binary.Write(encoded, binary.LittleEndian, block)
	batch := new(leveldb.Batch)
	//hash := bytes.NewBuffer(nil)
	//binary.Write(hash, binary.LittleEndian, block.Hash)
	//batch.Put(hash.Bytes(), encoded.Bytes())

	option := opt.WriteOptions{}
	option.Sync = true
	if block.Hash == 0 {
		balance := uint64(1000000000)
		encoded.Reset()
		binary.Write(encoded, binary.LittleEndian, balance)
		buf := encoded.Bytes()
		self.db.Put([]byte("a"), buf, &option)
	}

	for _, tx := range block.Transactions {
		from := self.Balance([]byte(tx.From))
		if from >= tx.Value {
			to := self.Balance([]byte(tx.To))
			from -= tx.Value
			to += tx.Value

			encoded.Reset()
			binary.Write(encoded, binary.LittleEndian, from)
			batch.Put([]byte(tx.From), encoded.Bytes())

			encoded.Reset()
			binary.Write(encoded, binary.LittleEndian, to)
			batch.Put([]byte(tx.To), encoded.Bytes())
		}
	}
	self.db.Write(batch, &option)
}

func (self *BlockChain) Balance(account []byte) uint64 {
	value, err := self.db.Get(account, nil)
	if err == leveldb.ErrNotFound {
		return 0
	}

	return binary.LittleEndian.Uint64(value)
}

func main() {
	//go func() {
	//	fmt.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
	datadir := "testdata"
	os.RemoveAll(datadir)

	issuer := "a"

	N := 100000
	prefixCount := 60
	prefix := strings.Repeat("x", prefixCount)
	accounts := make([]string, N)
	txes := make([]*Transaction, N)
	for i := 0; i < N; i++ {
		accounts[i] = fmt.Sprint(prefix, i)
		txes[i] = NewTransaction(issuer, accounts[i], 1000)
	}

	chain := NewBlockChain(datadir)
	block := &Block{Transactions: txes, Hash: 0}
	chain.Save(block)

	for i := 0; i < 200; i++ {
		txes := make([]*Transaction, N)
		for k := 0; k < N; k++ {
			from := accounts[rand.Int()%N]
			to := accounts[rand.Int()%N]

			txes[k] = NewTransaction(from, to, 1)
		}

		block := &Block{Transactions: txes, Hash: uint64(i + 1)}
		now := time.Now().UnixNano()
		chain.Save(block)
		newNow := time.Now().UnixNano()

		fmt.Printf("execution time:%d ms\n", (newNow-now)/1000000)
	}
}
