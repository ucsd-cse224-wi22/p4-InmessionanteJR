package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// basic logic refers professor's response in https://piazza.com/class/kxwl1taq8t1ql?cid=425
	// scan the base directory, and for each file, compute that file’s hash list
	local_Filehashlists := ComputeFileHashlist(client)
	// fmt.Println("local_Filehashlists: ", local_Filehashlists)

	// git add, add local unadded file to local index
	local_FileInfoMap := GitAdd(local_Filehashlists, client.BaseDir)
	// fmt.Println("local_FileInfoMap0: ", local_FileInfoMap)

	// get remote_FileInfoMap
	var remote_FileInfoMap map[string]*FileMetaData
	err := client.GetFileInfoMap(&remote_FileInfoMap)
	if err != nil {
		log.Panicln("Error occured when call client.GetFileInfoMap API!", err)
	}
	// fmt.Println("remote_FileInfoMap: ", remote_FileInfoMap)
	// compare the local version number to the remote version number

	// download (pull)
	for filename, remote_meta_data := range remote_FileInfoMap {
		_, ok := local_FileInfoMap[filename]
		// if !ok || (*remote_meta_data_ptr).Version > local_FileInfoMap[filename].Version {
		if !ok || remote_meta_data.Version > local_FileInfoMap[filename].Version {
			Download_helper(client, filename, &local_FileInfoMap, &remote_FileInfoMap)
		}
	}

	// upload (commit)
	for filename, local_meta_data := range local_FileInfoMap {
		_, ok := remote_FileInfoMap[filename]
		if !ok || local_meta_data.Version == remote_FileInfoMap[filename].Version+1 {
			Upload_helper(client, filename, &local_FileInfoMap)
		} else {
			// someone update the server, and I upload the local, now the local file and the remote file have the same version, but different content
			Download_helper(client, filename, &local_FileInfoMap, &remote_FileInfoMap)
		}
	}

	// update index.txt
	buff_content := ""
	// fmt.Println("local_FileInfoMap: ", local_FileInfoMap)
	for _, local_meta_data := range local_FileInfoMap {
		buff_content += local_meta_data.Filename
		buff_content += ","
		buff_content += strconv.Itoa(int(local_meta_data.Version))
		buff_content += ", "
		for _, i := range local_meta_data.BlockHashList {
			buff_content += i
			buff_content += " "
		}
		buff_content = buff_content[:len(buff_content)-1]
		buff_content += "\n"
	}
	// fmt.Println("buff_content: ", buff_content)
	if len(buff_content) > 0 {
		buff_content = buff_content[:len(buff_content)-1]
	}
	ioutil.WriteFile(client.BaseDir+"/index.txt", []byte(buff_content), 0644)
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
				// fmt.Println("bytes: ", bytes)
				if err != nil {
					if err == io.EOF {
						break
					} else {
						log.Panicln("Read file error!", err)
					}
				}
				hashBytes := sha256.Sum256(buffer[:bytes])
				hashString := hex.EncodeToString(hashBytes[:])
				local_hashlist = append(local_hashlist, hashString)
			}
			FileHashlists[file.Name()] = local_hashlist
			// fmt.Println("local_hashlist length when upload: ", len(local_hashlist))
		}
	}
	return FileHashlists
}

func GitAdd(local_Filehashlists map[string][]string, BaseDir string) map[string]FileMetaData {
	local_meta_map := make(map[string]FileMetaData)
	_, err := os.Stat(BaseDir + "/index.txt")
	if os.IsNotExist(err) {
		for filename, filehashlist := range local_Filehashlists {
			local_meta_map[filename] = FileMetaData{Filename: filename, Version: 1, BlockHashList: filehashlist}
		}
		// fmt.Println("local_meta_map", local_meta_map)
		return local_meta_map
	} else if err != nil {
		log.Panicln("Error occured when reading current dir in GitAdd!", err)
		return nil
	} else {
		fi, err := os.Open(BaseDir + "/index.txt")
		if err != nil {
			log.Panicln("Error occured when reading index.txt!", err)
		}
		defer fi.Close()
		br := bufio.NewReader(fi)
		for {
			line, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			fmt.Println("line: ", line)
			decoded_line := strings.Split(string(line), ",")
			fmt.Println("decoded_line: ", decoded_line)
			local_filename := decoded_line[0]
			local_version, _ := strconv.Atoi(decoded_line[1])
			local_hashlist := strings.Split(decoded_line[2], " ")
			local_meta_map[local_filename] = FileMetaData{Filename: local_filename, Version: int32(local_version), BlockHashList: local_hashlist}
		}
		for filename, local_hashlist := range local_Filehashlists {
			if _, ok := local_meta_map[filename]; !ok {

				// (1) there are now new files in the base directory that aren’t in the index file
				local_meta_map[filename] = FileMetaData{Filename: filename, Version: 1, BlockHashList: local_hashlist}
			} else {

				// (2) files that are in the index file, but have changed since the last time the client was executed
				if !CompareHashlist(local_meta_map[filename].BlockHashList, local_hashlist) {
					local_meta_map[filename] = FileMetaData{Filename: filename, Version: local_meta_map[filename].Version + 1, BlockHashList: local_hashlist}
				}
			}
		}

		// when one file is in index.txt, but not in the curr dir, this file is deleted
		for filename := range local_meta_map {
			if _, ok := local_Filehashlists[filename]; !ok {
				local_meta_map[filename] = FileMetaData{Filename: filename, Version: local_meta_map[filename].Version + 1, BlockHashList: []string{"0"}}
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

func Download_helper(client RPCClient, filename string, local_FileInfoMap *map[string]FileMetaData, remote_FileInfoMap *map[string]*FileMetaData) {
	// the current file is a delete file
	deleted_flag := false
	if len((*remote_FileInfoMap)[filename].BlockHashList) > 0 && (*remote_FileInfoMap)[filename].BlockHashList[0] == "0" {
		deleted_flag = true
	}
	if deleted_flag {
		_, err := os.Stat(client.BaseDir + "/" + filename)
		if err == nil {
			err := os.Remove(client.BaseDir + "/" + filename)
			if err != nil {
				log.Panicln("Error occured when delete file!", err)
			} else {
				fmt.Println("Delete file successfully!")
				return
			}
		}
	}

	// get blocks
	remote_hash_list := (*remote_FileInfoMap)[filename].BlockHashList
	// fmt.Println("length for remote_hash_list when download: ", len(remote_hash_list))
	var BlockStoreAddr string
	err := client.GetBlockStoreAddr(&BlockStoreAddr)
	if err != nil {
		log.Panicln("Error occured when call client.GetBlockStoreAddr API!", err)
	}
	hash_block_lists := (*local_FileInfoMap)[filename].BlockHashList

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
	(*local_FileInfoMap)[filename] = *(*remote_FileInfoMap)[filename]
}

func Upload_helper(client RPCClient, filename string, local_FileInfoMap *map[string]FileMetaData) {
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
	blocks_map := GetBlocksHelper(client, filename)
	for _, key := range (*local_FileInfoMap)[filename].BlockHashList {
		if _, ok := remote_exist_hash_list_set[key]; !ok {
			tmp := blocks_map[key]
			var succ bool
			client.PutBlock(&tmp, BlockStoreAddr, &succ)
		}
	}

	// upload remote index
	var latestVersion int32
	tmp := (*local_FileInfoMap)[filename]
	// fmt.Println("tmp", tmp)
	err = client.UpdateFile(&tmp, &latestVersion)
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
