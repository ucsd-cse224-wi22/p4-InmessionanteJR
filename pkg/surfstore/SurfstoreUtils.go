package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// basic logic refers professor's response in https://piazza.com/class/kxwl1taq8t1ql?cid=425

	// scan the base directory, and for each file, compute that file’s hash list
	local_Filehashlists := ComputeFileHashlist(client)

	// git add, add local unadded file to local index (treating this as commit is also ok)
	local_FileInfoMap := GitAdd(client, local_Filehashlists, client.BaseDir)

	// get remote_FileInfoMap
	var remote_FileInfoMap map[string]*FileMetaData
	err := client.GetFileInfoMap(&remote_FileInfoMap)
	if err != nil {
		log.Panicln("Error occured when call client.GetFileInfoMap API!", err)
	}

	// compare the local version number to the remote version number
	// (1) download (pull)
	for filename, remote_meta_data := range remote_FileInfoMap {
		map_value, ok := local_FileInfoMap[filename]
		if !ok || remote_meta_data.Version > map_value.Version {
			Download_helper(client, filename, &local_FileInfoMap, &remote_FileInfoMap)
		} else if remote_meta_data.Version == map_value.Version && !CompareHashlist(map_value.BlockHashList, remote_meta_data.BlockHashList) {
			// race condition
			// someone update the server, and I upload the local, now the local file and the remote file have the same version, but different content
			Download_helper(client, filename, &local_FileInfoMap, &remote_FileInfoMap)
		}
	}

	// (2) upload (push)
	for filename, local_meta_data := range local_FileInfoMap {
		// deleted or not
		deleted_flag := false
		if len(local_FileInfoMap[filename].BlockHashList) == 1 && local_FileInfoMap[filename].BlockHashList[0] == "0" {
			deleted_flag = true
		}

		map_value, ok := remote_FileInfoMap[filename]
		if !ok || (local_meta_data.Version == map_value.Version+1) {
			Upload_helper(client, filename, &local_FileInfoMap, deleted_flag)
		} else if local_meta_data.Version > map_value.Version+1 {
			log.Panicln("Local version is larger than 1 compared than remote version, which is imp!", err)
		}
	}

	// update index.txt
	err = WriteMetaFile(local_FileInfoMap, client.BaseDir)
	if err != nil {
		log.Fatal("Error when call WriteMetaFile api!")
	}
}

func badStringError(what, val string) error {
	return fmt.Errorf("%s %q", what, val)
}

func ComputeFileHashlist(client RPCClient) (FileHashlists map[string][]string) {
	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Panicln("Error occured when reading current dir!", err)
	}
	FileHashlists = make(map[string][]string)
	for _, file := range files {
		if file.IsDir() {
			log.Panicln("Subdir exists in current dir!", err)
		} else if file.Name() == "index.txt" || file.Name() == ".DS_Store" {
			// } else if file.Name() == "index.txt" {
			continue
		} else {
			f, _ := os.Open(client.BaseDir + "/" + file.Name())
			defer f.Close()
			local_hashlist := make([]string, 0)
			for {
				buffer := make([]byte, client.BlockSize)
				bytes, err := f.Read(buffer)
				if err != nil {
					if err == io.EOF {
						break
					} else {
						log.Panicln("Read file error!", err)
					}
				}
				local_hashlist = append(local_hashlist, GetBlockHashString(buffer[:bytes]))
			}
			FileHashlists[file.Name()] = local_hashlist
		}
	}
	return FileHashlists
}

func GitAdd(client RPCClient, local_Filehashlists map[string][]string, BaseDir string) map[string]*FileMetaData {
	local_meta_map := make(map[string]*FileMetaData)
	_, err := os.Stat(BaseDir + "/index.txt")
	if os.IsNotExist(err) {
		for filename, filehashlist := range local_Filehashlists {
			local_meta_map[filename] = &FileMetaData{Filename: filename, Version: 1, BlockHashList: filehashlist}
		}
		return local_meta_map
	} else if err != nil {
		log.Panicln("Error occured when reading current dir in GitAdd!", err)
		return nil
	} else {
		local_meta_map, err := LoadMetaFromMetaFile(BaseDir)
		if err != nil {
			log.Panicln("Error occured when call LoadMetaFromMetaFile api!", err)
		}
		for filename, local_hashlist := range local_Filehashlists {
			if map_value, ok := local_meta_map[filename]; !ok {
				// (1) there are now new files in the base directory that aren’t in the index file
				local_meta_map[filename] = &FileMetaData{Filename: filename, Version: 1, BlockHashList: local_hashlist}
			} else {
				// (2) files that are in the index file, but have changed since the last time the client was executed
				if !CompareHashlist(map_value.BlockHashList, local_hashlist) {
					local_meta_map[filename] = &FileMetaData{Filename: filename, Version: map_value.Version + 1, BlockHashList: local_hashlist}
				}
			}
		}

		// when one file is in index.txt, but not in the curr dir, this file is deleted
		for filename := range local_meta_map {
			if _, ok := local_Filehashlists[filename]; !ok {
				// get remote_FileInfoMap
				var remote_FileInfoMap map[string]*FileMetaData
				err := client.GetFileInfoMap(&remote_FileInfoMap)
				if err != nil {
					log.Panicln("Error occured when call client.GetFileInfoMap API!", err)
				}
				if _, ok := remote_FileInfoMap[filename]; ok && (len(remote_FileInfoMap[filename].BlockHashList) == 1 && remote_FileInfoMap[filename].BlockHashList[0] == "0") {
					local_meta_map[filename] = &FileMetaData{Filename: filename, Version: local_meta_map[filename].Version, BlockHashList: []string{"0"}}
				} else {
					local_meta_map[filename] = &FileMetaData{Filename: filename, Version: local_meta_map[filename].Version + 1, BlockHashList: []string{"0"}}
				}
			}
		}
		return local_meta_map
	}
}

func CompareHashlist(hashlist1 []string, hashlist2 []string) bool {
	if len(hashlist1) != len(hashlist2) {
		return false
	}
	for i := 0; i < len(hashlist1); i++ {
		if hashlist1[i] != hashlist2[i] {
			return false
		}
	}
	return true
}

func Download_helper(client RPCClient, filename string, local_FileInfoMap *map[string]*FileMetaData, remote_FileInfoMap *map[string]*FileMetaData) {
	// the current file is a deleted file
	deleted_flag := false
	if len((*remote_FileInfoMap)[filename].BlockHashList) == 1 && (*remote_FileInfoMap)[filename].BlockHashList[0] == "0" {
		deleted_flag = true
	}

	if deleted_flag {
		(*local_FileInfoMap)[filename] = &FileMetaData{Filename: filename, Version: (*local_FileInfoMap)[filename].Version + 1, BlockHashList: []string{"0"}}
		_, err := os.Stat(client.BaseDir + "/" + filename)
		if err == nil {
			err := os.Remove(client.BaseDir + "/" + filename)
			if err != nil {
				log.Panicln("Error occured when delete file!", err)
			} else {
				log.Println("Delete file successfully!")
				return
			}
		}
	}

	// get needed blocks from server
	remote_hash_list := (*remote_FileInfoMap)[filename].BlockHashList
	var BlockStoreAddr string
	err := client.GetBlockStoreAddr(&BlockStoreAddr)
	if err != nil {
		log.Panicln("Error occured when call client.GetBlockStoreAddr API!", err)
	}
	hash_block_lists := (*remote_FileInfoMap)[filename].BlockHashList
	local_block_map := make(map[string]Block)
	for _, hash_block_list := range hash_block_lists {
		var block Block
		client.GetBlock(hash_block_list, BlockStoreAddr, &block)
		local_block_map[hash_block_list] = block
	}

	// concat blocks to restore the file
	buff := make([]byte, 0)
	for _, hash := range remote_hash_list {
		buff = append(buff, local_block_map[hash].BlockData...)
	}
	ioutil.WriteFile(client.BaseDir+"/"+filename, buff, 0644)

	// update local_FileInfoMap
	(*local_FileInfoMap)[filename] = (*remote_FileInfoMap)[filename]
}

func Upload_helper(client RPCClient, filename string, local_FileInfoMap *map[string]*FileMetaData, deleted_flag bool) {
	if !deleted_flag {
		var BlockStoreAddr string
		err := client.GetBlockStoreAddr(&BlockStoreAddr)
		if err != nil {
			log.Panicln("Error occured when call client.GetBlockStoreAddr API!", err)
		}

		remote_exist_hash_list := make([]string, 0)
		client.HasBlocks((*local_FileInfoMap)[filename].BlockHashList, BlockStoreAddr, &remote_exist_hash_list)

		remote_exist_hash_list_set := make(map[string]bool)
		// go doesn't support in-built set, so we use hashmap
		for _, i := range remote_exist_hash_list {
			remote_exist_hash_list_set[i] = true
		}

		// upload blocks that doesn't exist in metastore server
		blocks_map := GetBlocksHelper(client, filename) // can optimize, only get a blocks_map which only contains keys that are not in client.HasBlocks()
		for _, key := range (*local_FileInfoMap)[filename].BlockHashList {
			if _, ok := remote_exist_hash_list_set[key]; !ok {
				tmp := blocks_map[key]
				var succ bool
				client.PutBlock(&tmp, BlockStoreAddr, &succ)
				if !succ {
					log.Panicln("Error occured when call client.PutBlock API!")
				}
			}
		}
	}

	// upload remote index
	var latestVersion int32
	tmp := (*local_FileInfoMap)[filename]
	err := client.UpdateFile(tmp, &latestVersion)
	if err != nil {
		log.Panicln("Error occured when call client.UpdateFile API!", err)
	}
}

func GetBlocksHelper(client RPCClient, filename string) (block_map map[string]Block) {
	f, _ := os.Open(client.BaseDir + "/" + filename)
	defer f.Close()
	block_map = make(map[string]Block)
	for {
		buffer := make([]byte, client.BlockSize)
		bytes, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Panicln("Read file error!", err)
			}
		}
		hashBytes := sha256.Sum256(buffer[:bytes])
		hashString := hex.EncodeToString(hashBytes[:])
		block_map[hashString] = Block{BlockData: buffer[:bytes], BlockSize: int32(bytes)}
	}
	return block_map
}
