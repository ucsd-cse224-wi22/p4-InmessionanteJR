package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mutex          sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	filename := (*fileMetaData).Filename
	rmt_meta_data, ok := m.FileMetaMap[filename]
	if ok {
		// update
		if fileMetaData.Version == rmt_meta_data.Version+1 {
			m.FileMetaMap[filename] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			return nil, badStringError("Invalid version", string(fileMetaData.Version))
		}
	} else {
		// new
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
