package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mutex    sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	block, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return nil, badStringError("Bad entry in bs.BlockMap!", "")
	}
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	hashBytes := sha256.Sum256(block.BlockData)
	hashString := hex.EncodeToString(hashBytes[:])
	bs.BlockMap[hashString] = block
	return &Success{Flag: true}, nil // when we return false?
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	local_bh := BlockHashes{Hashes: make([]string, 0)}
	for _, i := range (*blockHashesIn).Hashes {
		if _, ok := bs.BlockMap[i]; ok {
			local_bh.Hashes = append(local_bh.Hashes, i)
		}
	}
	return &local_bh, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
