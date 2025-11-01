package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/hyperledger/client/pkg/fabric"
	"github.com/hyperledger/storage/chunk"
	"github.com/hyperledger/storage/pipeline"
)

// 全局变量
var (
	HashStringMap map[uint64]string
	cm            *chunk.ChunkManager
	nodeTrees     map[string]*merkletree.MerkleTree // 每个节点的默克尔树
	pipelineMgr   *pipeline.Pipeline
	fabricClient  *fabric.FabricClient

	// 按节点防抖触发上链
	debounceInterval time.Duration
	nodeTimers       map[string]*time.Timer
	mu               sync.Mutex

	pendingMerkleRoots map[string]bool // 只存节点名、不记录值
	pendingMutex       sync.Mutex
	batchTicker        *time.Ticker
	batchInterval      time.Duration = 10 * time.Second // 可通过env调整
)

const (
	MessageHeaderFlag = 0xA5 // 固定消息标识 1010 0101
)

// FileContent 实现 merkletree.Content 接口
type FileContent struct {
	FileName string
	Data     []byte
	hash     []byte
}

func NewFileContent(fileName string, data []byte) *FileContent {
	return &FileContent{
		FileName: fileName,
		Data:     data,
	}
}

// CalculateHash 计算内容的哈希
func (f *FileContent) CalculateHash() ([]byte, error) {
	if f.hash != nil {
		return f.hash, nil
	}

	// 只用内容，不加文件名
	hash := sha256.New()
	// hash.Write([]byte(f.FileName)) // 不再拼接文件名
	hash.Write(f.Data)
	f.hash = hash.Sum(nil)

	return f.hash, nil
}

// Equals 比较两个内容是否相等
func (f *FileContent) Equals(other merkletree.Content) (bool, error) {
	otherContent, ok := other.(*FileContent)
	if !ok {
		return false, nil
	}

	// 比较文件名和数据
	if f.FileName != otherContent.FileName {
		return false, nil
	}

	// 比较数据长度
	if len(f.Data) != len(otherContent.Data) {
		return false, nil
	}

	// 比较数据内容
	for i := range f.Data {
		if f.Data[i] != otherContent.Data[i] {
			return false, nil
		}
	}

	return true, nil
}

// 读取完整消息
func readMessage(conn net.Conn) ([]byte, error) {
	// 读取消息头 (4字节)
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("failed to read message header: %v", err)
	}

	// 验证消息头标识
	if header[0] != MessageHeaderFlag {
		return nil, fmt.Errorf("invalid message header flag: %x", header[0])
	}

	// 解析消息长度 (后3字节)
	messageLength := uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])

	// 读取消息体
	messageBody := make([]byte, messageLength)
	if _, err := io.ReadFull(conn, messageBody); err != nil {
		return nil, fmt.Errorf("failed to read message body: %v", err)
	}

	return messageBody, nil
}

// 发送消息
func sendMessage(conn net.Conn, data []byte) error {
	// 构造消息头
	header := make([]byte, 4)
	header[0] = MessageHeaderFlag
	length := uint32(len(data))
	header[1] = byte((length >> 16) & 0xFF)
	header[2] = byte((length >> 8) & 0xFF)
	header[3] = byte(length & 0xFF)

	// 先发送消息头
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("failed to send message header: %v", err)
	}

	// 发送消息体
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to send message body: %v", err)
	}

	return nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		messageBody, err := readMessage(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client closed connection")
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("Connection timeout")
				return
			}
			log.Printf("Error reading message: %v", err)
			return
		}

		if len(messageBody) == 0 {
			log.Printf("Empty message body")
			continue
		}

		opType := messageBody[0]
		messageData := messageBody[1:]

		switch opType {
		case 0: // 上传操作
			handleUpload(conn, messageData)
		case 1: // 下载操作
			handleDownload(conn, messageData)
		default:
			log.Printf("Unknown operation type: %d", opType)
			sendErrorMessage(conn, "Unknown operation type")
		}
	}
}

// 发送错误消息
func sendErrorMessage(conn net.Conn, message string) {
	response := []byte("ERROR: " + message)
	conn.Write(response)
}

// 处理文件上传
// func handleUpload(conn net.Conn, data []byte) {
// 	if len(data) < 8+16+4 {
// 		log.Printf("Upload message too short")
// 		sendErrorMessage(conn, "Message too short")
// 		return
// 	}

// 	// 解析上传数据
// 	fileHash := binary.BigEndian.Uint64(data[0:8])
// 	fileNameBytes := data[8:24]
// 	fileName := string(bytes.TrimRight(fileNameBytes, "\x00"))
// 	fileLength := binary.BigEndian.Uint32(data[24:28])

// 	if uint32(len(data[28:])) < fileLength {
// 		log.Printf("File content incomplete")
// 		sendErrorMessage(conn, "File content incomplete")
// 		return
// 	}

// 	fileContent := data[28 : 28+fileLength]

// 	// 从文件名提取节点名 (假设文件名格式为 "nodeX_filename")
// 	nodeName := extractNodeName(fileName)
// 	if nodeName == "" {
// 		nodeName = "node0" // 默认节点
// 	}

// 	// 同步写入文件到块存储
// 	if err := cm.WriteObject(nodeName, fileName, fileContent); err != nil {
// 		log.Printf("Error writing object to chunk: %v", err)
// 		sendErrorMessage(conn, err.Error())
// 		return
// 	}

// 	// 更新哈希映射
// 	HashStringMap[fileHash] = fileName

// 	// 更新节点的默克尔树
// 	if err := updateNodeMerkleTree(nodeName, fileName, fileContent); err != nil {
// 		log.Printf("Error updating merkle tree for node %s: %v", nodeName, err)
// 	}
// }

// 处理文件上传
func handleUpload(conn net.Conn, data []byte) {
	// 更新长度检查：8字节文件哈希 + 8字节节点名 + 16字节文件名 + 4字节文件长度
	if len(data) < 8+8+16+4 {
		log.Printf("Upload message too short: expected at least %d bytes, got %d bytes", 8+8+16+4, len(data))
		sendErrorMessage(conn, "Message too short")
		return
	}

	// 解析上传数据
	offset := 0

	// 文件哈希 (8字节)
	fileHash := binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// 节点名 (8字节)
	nodeNameBytes := data[offset : offset+8]
	nodeName := string(bytes.TrimRight(nodeNameBytes, "\x00"))
	offset += 8

	// 文件名 (16字节)
	fileNameBytes := data[offset : offset+16]
	fileName := string(bytes.TrimRight(fileNameBytes, "\x00"))
	offset += 16

	// 文件长度 (4字节)
	fileLength := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// 检查文件内容是否完整
	if uint32(len(data[offset:])) < fileLength {
		log.Printf("File content incomplete: expected %d bytes, got %d bytes", fileLength, len(data[offset:]))
		sendErrorMessage(conn, "File content incomplete")
		return
	}

	// 文件内容
	fileContent := data[offset : offset+int(fileLength)]

	log.Printf("Upload request - Hash: %d, Node: %s, File: %s, Length: %d bytes",
		fileHash, nodeName, fileName, fileLength)

	// 验证节点名格式
	if !isValidNodeName(nodeName) {
		log.Printf("Invalid node name: %s", nodeName)
		sendErrorMessage(conn, "Invalid node name")
		return
	}

	// 同步写入文件到块存储
	if err := cm.WriteObject(nodeName, fileName, fileContent); err != nil {
		log.Printf("Error writing object to chunk: %v", err)
		sendErrorMessage(conn, err.Error())
		return
	}

	// 更新哈希映射
	HashStringMap[fileHash] = fileName

	// 更新节点的默克尔树
	if err := updateNodeMerkleTree(nodeName, fileName, fileContent); err != nil {
		log.Printf("Error updating merkle tree for node %s: %v", nodeName, err)
	}

	// 发送成功响应
	/* 	response := []byte("SUCCESS: File uploaded successfully")
	   	if err := sendMessage(conn, response); err != nil {
	   		log.Printf("Error sending success response: %v", err)
	   	} else {
	   		log.Printf("Successfully uploaded file %s to node %s (hash: %d, size: %d bytes)",
	   			fileName, nodeName, fileHash, fileLength)
	   	} */
}

// 验证节点名格式的辅助函数
func isValidNodeName(nodeName string) bool {
	if len(nodeName) == 0 || len(nodeName) > 8 {
		return false
	}

	// 检查是否以 "node" 开头，后面跟数字
	if !strings.HasPrefix(nodeName, "node") {
		return false
	}

	// 检查 "node" 后面的部分是否都是数字
	nodeNumStr := strings.TrimPrefix(nodeName, "node")
	if nodeNumStr == "" {
		return false
	}

	for _, char := range nodeNumStr {
		if char < '0' || char > '9' {
			return false
		}
	}

	return true
}

// 可以移除原来的 extractNodeName 函数，因为现在节点名直接从协议中获取
// 处理文件下载
func handleDownload(conn net.Conn, data []byte) {
	if len(data) < 8+8+1 {
		log.Printf("Download message too short")
		sendErrorMessage(conn, "Message too short")
		return
	}

	// 解析下载数据
	nodeNameBytes := data[0:8]
	nodeName := string(bytes.TrimRight(nodeNameBytes, "\x00"))
	fileHash := binary.BigEndian.Uint64(data[8:16])
	needProof := data[16] // 0-不需要证明，1-需要证明

	// 查找文件名
	fileName, exists := HashStringMap[fileHash]
	if !exists {
		log.Printf("File with hash %d not found", fileHash)
		sendDownloadResponse(conn, fileHash, false, nil, false, nil, nil)
		return
	}

	// 使用流水线处理下载请求
	request := &pipeline.DownloadRequest{
		FileName:  fileName,
		FileHash:  fileHash,
		Conn:      conn,
		NeedProof: needProof == 1,
		NodeName:  nodeName,
	}

	if pipelineMgr.SubmitDownloadRequest(request) {
		log.Printf("Download request submitted to pipeline: %s (node: %s, needProof: %v)",
			fileName, nodeName, needProof == 1)
	} else {
		// 如果流水线繁忙，回退到同步处理
		log.Printf("Pipeline busy, falling back to synchronous download for: %s (node: %s)", fileName, nodeName)
		handleDownloadSync(conn, fileName, fileHash, nodeName, needProof == 1)
	}
}

// 发送下载响应
func sendDownloadResponse(conn net.Conn, fileHash uint64, found bool, fileData []byte,
	hasProof bool, merklePath [][]byte, indices []int64) error {

	// log.Printf("这是在干什么\n")
	// // [MERKLE-DEBUG] Begin debug info
	// if fileData != nil && len(merklePath) > 0 {
	// 	// 手动计算 leafHash
	// 	leafHasher := sha256.New()
	// 	// leafHasher.Write([]byte(fileName))
	// 	leafHasher.Write(fileData)
	// 	leafHash := leafHasher.Sum(nil)

	// 	fmt.Printf("[MERKLE-DEBUG] leafHash: %x\n", leafHash)
	// 	for i, ph := range merklePath {
	// 		fmt.Printf("[MERKLE-DEBUG] path[%d]: %x\n", i, ph)
	// 	}
	// 	fmt.Printf("[MERKLE-DEBUG] indices: %v\n", indices)

	// 	currentHash := leafHash
	// 	for i, siblingHash := range merklePath {
	// 		direction := indices[i]
	// 		var combinedHash []byte
	// 		if direction == 1 {
	// 			combinedHash = append(currentHash, siblingHash...)
	// 		} else {
	// 			combinedHash = append(siblingHash, currentHash...)
	// 		}
	// 		hasher := sha256.New()
	// 		hasher.Write(combinedHash)
	// 		currentHash = hasher.Sum(nil)
	// 		fmt.Printf("[MERKLE-DEBUG] after level %d: %x\n", i, currentHash)
	// 	}
	// 	// expectedRoot 通常由merkle tree给出，这里无法直接获得，只打印
	// 	fmt.Printf("[MERKLE-DEBUG] manually calculated root: %x\n", currentHash)
	// }
	// [MERKLE-DEBUG] End debug info

	// 构造响应消息体
	var response []byte

	// 文件哈希 (8字节)
	hashBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashBytes, fileHash)
	response = append(response, hashBytes...)

	// 是否找到数据 (1字节)
	if found {
		response = append(response, 1)
	} else {
		response = append(response, 0)
		// 如果没有找到，直接发送
		return sendMessage(conn, response)
	}

	// 文件长度 (4字节) 和文件数据
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(fileData)))
	response = append(response, lengthBytes...)
	response = append(response, fileData...)

	// 是否有证明 (1字节)
	if hasProof {
		response = append(response, 1)
	} else {
		response = append(response, 0)
		return sendMessage(conn, response)
	}

	// Merkle Path 长度 (2字节)
	pathLength := uint16(len(merklePath))
	pathLengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(pathLengthBytes, pathLength)
	response = append(response, pathLengthBytes...)

	// Merkle Path 数据 (每个哈希32字节)
	for _, pathHash := range merklePath {
		response = append(response, pathHash...)
	}

	// Indices 数组 (每个1字节)
	for _, index := range indices {
		response = append(response, byte(index))
	}

	return sendMessage(conn, response)
}

// 回退下载 - 同步处理
func handleDownloadSync(conn net.Conn, fileName string, fileHash uint64, nodeName string, needProof bool) {
	// 从块中读取对象
	value, err := cm.ReadObject(fileName)
	if err != nil {
		log.Printf("Error reading object from chunk: %v", err)
		sendDownloadResponse(conn, fileHash, false, nil, false, nil, nil)
		return
	}

	var merklePath [][]byte
	var indices []int64
	hasProof := false

	// 如果需要证明，生成默克尔树证明
	if needProof {
		merklePath, indices, err = generateMerkleProof(nodeName, fileName, value)
		if err != nil {
			log.Printf("Error generating merkle proof: %v", err)
		} else {
			hasProof = true
			log.Printf("Generated merkle proof for %s: %d path elements, %d indices",
				fileName, len(merklePath), len(indices))
		}
	}

	// 发送响应
	if err := sendDownloadResponse(conn, fileHash, true, value, hasProof, merklePath, indices); err != nil {
		log.Printf("Error sending download response: %v", err)
	} else {
		log.Printf("Successfully sent object %s via sync download (proof: %v)", fileName, hasProof)
	}
}

// 从文件名提取节点名
func extractNodeName(fileName string) string {
	// 简单的提取逻辑：假设文件名格式为 "nodeX_filename"
	parts := strings.Split(fileName, "_")
	if len(parts) > 0 && strings.HasPrefix(parts[0], "node") {
		return parts[0]
	}
	return "node0" // 默认节点
}

// 更新节点的默克尔树
func updateNodeMerkleTree(nodeName, fileName string, content []byte) error {
	// 清除缓存，下次访问时重新构建
	delete(nodeTrees, nodeName)
	log.Printf("Updated merkle tree cache for node %s", nodeName)

	// 启动/重置该节点的防抖定时器，到期后再触发上链
	if fabricClient != nil {
		mu.Lock()
		if t, ok := nodeTimers[nodeName]; ok {
			if t.Stop() {
				// 尽量清理旧定时器回调
			}
		}
		timer := time.AfterFunc(debounceInterval, func() {
			processNodeMerkleUpdate(nodeName)
		})
		nodeTimers[nodeName] = timer
		mu.Unlock()
	}
	return nil
}

// // 生成默克尔树证明
// func generateMerkleProof(nodeName, fileName string, content []byte) ([][]byte, []int64, error) {
// 	tree, err := getOrCreateMerkleTree(nodeName)
// 	if err != nil {
// 		return nil, nil, fmt.Errorf("failed to get merkle tree: %v", err)
// 	}

// 	// 创建内容对象 - 使用实际的文件名和内容
// 	fileContent := NewFileContent(fileName, content)

// 	// 生成证明
// 	proof, indices, err := tree.GetMerklePath(fileContent)
// 	if err != nil {
// 		return nil, nil, fmt.Errorf("failed to generate merkle path: %v", err)
// 	}

// 	// 提取路径哈希
// 	var merklePath [][]byte
// 	for _, p := range proof {
// 		merklePath = append(merklePath, p)
// 	}

// 	log.Printf("Generated merkle proof for %s: %d path elements, %d indices",
// 		fileName, len(merklePath), len(indices))

// 	return merklePath, indices, nil
// }

func generateMerkleProof(nodeName, fileName string, content []byte) ([][]byte, []int64, error) {
	tree, err := getOrCreateMerkleTree(nodeName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get merkle tree: %v", err)
	}

	// 创建内容对象 - 使用实际的文件名和内容
	fileContent := NewFileContent(fileName, content)

	// 生成证明
	proof, indices, err := tree.GetMerklePath(fileContent)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate merkle path: %v", err)
	}

	// 提取路径哈希
	var merklePath [][]byte
	for _, p := range proof {
		merklePath = append(merklePath, p)
	}

	log.Printf("[MERKLE-DEBUG] ---- generateMerkleProof ----")
	log.Printf("[MERKLE-DEBUG] fileName: %s", fileName)

	// leaf hash算法要与客户端和树完全一致
	leafHasher := sha256.New()
	// leafHasher.Write([]byte(fileName))
	leafHasher.Write(content)
	leafHash := leafHasher.Sum(nil)

	log.Printf("[MERKLE-DEBUG] leafHash: %x", leafHash)

	for i, ph := range merklePath {
		log.Printf("[MERKLE-DEBUG] path[%d]: %x", i, ph)
	}
	log.Printf("[MERKLE-DEBUG] indices: %v", indices)

	// 手动计算 expected root
	currentHash := leafHash
	for i, siblingHash := range merklePath {
		direction := indices[i]
		var combinedHash []byte
		if direction == 1 {
			combinedHash = append(currentHash, siblingHash...)
		} else {
			combinedHash = append(siblingHash, currentHash...)
		}
		hasher := sha256.New()
		hasher.Write(combinedHash)
		currentHash = hasher.Sum(nil)
		log.Printf("[MERKLE-DEBUG] after level %d: %x", i, currentHash)
	}
	expectedRoot := tree.MerkleRoot()
	log.Printf("[MERKLE-DEBUG] manually calculated root: %x", currentHash)
	log.Printf("[MERKLE-DEBUG] merkleTree root:         %x", expectedRoot)
	log.Printf("[MERKLE-DEBUG] compare: %v", bytes.Equal(currentHash, expectedRoot))

	log.Printf("Generated merkle proof for %s: %d path elements, %d indices", fileName, len(merklePath), len(indices))
	return merklePath, indices, nil
}

// // 获取或创建节点的默克尔树
// func getOrCreateMerkleTree(nodeName string) (*merkletree.MerkleTree, error) {
// 	if tree, exists := nodeTrees[nodeName]; exists {
// 		return tree, nil
// 	}

// 	// 从存储中构建默克尔树
// 	contents, err := buildMerkleContentsFromStorage(nodeName)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// 如果没有文件，创建一个空的默克尔树
// 	if len(contents) == 0 {
// 		contents = []merkletree.Content{NewFileContent("empty", []byte(""))}
// 	}

// 	tree, err := merkletree.NewTree(contents)
// 	if err != nil {
// 		return nil, err
// 	}

// 	nodeTrees[nodeName] = tree

// 	// 计算根哈希用于验证
// 	rootHash := tree.MerkleRoot()
// 	log.Printf("Built merkle tree for node %s with root: %s", nodeName, hex.EncodeToString(rootHash))

//		return tree, nil
//	}
//
// 获取或创建节点的默克尔树 - 优化版本
func getOrCreateMerkleTree(nodeName string) (*merkletree.MerkleTree, error) {
	if tree, exists := nodeTrees[nodeName]; exists {
		return tree, nil
	}

	// 从存储中构建默克尔树
	contents, err := buildMerkleContentsFromStorage(nodeName)
	if err != nil {
		return nil, err
	}

	// 如果没有文件，创建一个空的默克尔树
	if len(contents) == 0 {
		contents = []merkletree.Content{NewFileContent("empty", []byte(""))}
	}

	tree, err := merkletree.NewTree(contents)
	if err != nil {
		return nil, err
	}

	nodeTrees[nodeName] = tree

	// 计算根哈希用于验证
	rootHash := tree.MerkleRoot()
	log.Printf("Built merkle tree for node %s with %d objects, root: %s",
		nodeName, len(contents), hex.EncodeToString(rootHash))

	return tree, nil
}

// 从存储构建默克尔树内容
// func buildMerkleContentsFromStorage(nodeName string) ([]merkletree.Content, error) {
// 	var contents []merkletree.Content

// 	// 读取节点目录下的所有文件
// 	nodeDir := filepath.Join("server", nodeName)

// 	// 检查节点目录是否存在
// 	if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
// 		return contents, nil
// 	}

// 	// 读取目录下的所有文件
// 	files, err := os.ReadDir(nodeDir)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read node directory: %v", err)
// 	}

// 	for _, file := range files {
// 		if !file.IsDir() {
// 			fileName := file.Name()
// 			filePath := filepath.Join(nodeDir, fileName)

// 			// 读取文件内容
// 			content, err := os.ReadFile(filePath)
// 			if err != nil {
// 				log.Printf("Warning: failed to read file %s: %v", filePath, err)
// 				continue
// 			}

// 			// 创建Content对象
// 			fileContent := NewFileContent(fileName, content)
// 			contents = append(contents, fileContent)

// 			log.Printf("Added file to merkle tree: %s (size: %d bytes)", fileName, len(content))
// 		}
// 	}

// 	log.Printf("Built merkle contents for node %s: %d files", nodeName, len(contents))
// 	return contents, nil
// }

// 从存储构建默克尔树内容 - 修改版本：使用chunk中的小文件作为叶子节点
func buildMerkleContentsFromStorage(nodeName string) ([]merkletree.Content, error) {
	var contents []merkletree.Content
	// 从ChunkManager获取该节点的所有对象
	objects, err := cm.GetNodeObjects(nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get node objects: %v", err)
	}
	var keys []string
	for k := range objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, objectKey := range keys {
		objectData := objects[objectKey]
		fileContent := NewFileContent(objectKey, objectData)
		contents = append(contents, fileContent)
		log.Printf("Added object to merkle tree: %s (size: %d bytes)", objectKey, len(objectData))
	}
	log.Printf("Built merkle contents for node %s: %d objects", nodeName, len(contents))
	return contents, nil
}

// 从磁盘读取构建默克尔树内容（避免并发访问内存结构导致的竞态）
func buildMerkleContentsFromDisk(baseDir, nodeName string) ([]merkletree.Content, error) {
	var contents []merkletree.Content

	nodeDir := filepath.Join(baseDir, nodeName)
	if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
		return contents, nil
	}

	entries, err := os.ReadDir(nodeDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read node directory: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// 跳过非数据文件（按需：若有明显前缀/后缀可过滤）
		filePath := filepath.Join(nodeDir, name)
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("Warning: failed to read %s: %v", filePath, err)
			continue
		}
		contents = append(contents, NewFileContent(name, data))
	}
	return contents, nil
}

// 串行处理上链更新：去重同一节点的重复请求，按磁盘快照重建树
// 到期触发：按节点重建并上链
func processNodeMerkleUpdate(nodeName string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in processNodeMerkleUpdate for %s: %v", nodeName, r)
		}
	}()

	baseDir := "server"
	contents, err := buildMerkleContentsFromDisk(baseDir, nodeName)
	if err != nil {
		log.Printf("failed to build merkle contents (disk) for %s: %v", nodeName, err)
		return
	}
	if len(contents) == 0 {
		contents = []merkletree.Content{NewFileContent("empty", []byte(""))}
	}
	tree, err := merkletree.NewTree(contents)
	if err != nil {
		log.Printf("failed to build merkle tree for %s: %v", nodeName, err)
		return
	}
	hex.EncodeToString(tree.MerkleRoot())

	if fabricClient != nil {
		pendingMutex.Lock()
		pendingMerkleRoots[nodeName] = true
		pendingMutex.Unlock()
		log.Printf("[BATCH] node %s ready for next batch", nodeName)
	}

	// 清理该节点的定时器引用
	mu.Lock()
	delete(nodeTimers, nodeName)
	mu.Unlock()
}

func computeRingID(nodeName string) string {
	// 环境变量优先（如单机或固定映射场景）
	if rid := os.Getenv("FABRIC_RING_ID"); rid != "" {
		return rid
	}
	// nodeName 形如 "node12"
	if strings.HasPrefix(nodeName, "node") {
		numStr := strings.TrimPrefix(nodeName, "node")
		if n, err := strconv.Atoi(numStr); err == nil {
			if n >= 0 && n < 40 {
				return fmt.Sprintf("ring%d", n%4)
			}
			if n >= 40 && n <= 49 {
				return "ring4"
			}
			if n >= 50 && n <= 59 {
				return "ring5"
			}
		}
	}
	// 兜底：ring1
	return "ring1"
}

func doBatchMerkleRoots() {
	pendingMutex.Lock()
	var nodes []string
	for node := range pendingMerkleRoots {
		nodes = append(nodes, node)
	}
	pendingMerkleRoots = make(map[string]bool)
	pendingMutex.Unlock()

	if len(nodes) == 0 || fabricClient == nil {
		return
	}

	var batch []fabric.BatchMerkleUpdateItem
	for _, node := range nodes {
		tree, err := getOrCreateMerkleTree(node)
		if err != nil {
			log.Printf("[BATCH] error building merkle for node %s: %v", node, err)
			continue
		}
		root := hex.EncodeToString(tree.MerkleRoot())
		ringID := computeRingID(node)
		log.Printf("[BATCH] node %s ring %s root %s (fresh calc)", node, ringID, root)
		batch = append(batch, fabric.BatchMerkleUpdateItem{
			RingID: ringID, ServerName: node, MerkleRootHash: root,
		})
	}
	if len(batch) > 0 {
		log.Printf("[BATCH] uploading to chain, %d merkle roots", len(batch))
		err := fabricClient.BatchUpdateMerkleRoots(batch)
		if err != nil {
			log.Printf("[BATCH] failed to update merkle roots: %v", err)
		} else {
			log.Printf("[BATCH] updated merkle roots for nodes: %v", func() []string {
				names := make([]string, 0, len(batch))
				for _, b := range batch {
					names = append(names, b.ServerName)
				}
				return names
			}())
		}
	}
}

// 读取处理器 - 供流水线调用
func readHandler(fileName string) ([]byte, error) {
	return cm.ReadObject(fileName)
}

// 启动服务器
func startServer(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Printf("Error listening on port %s: %v", port, err)
		return
	}
	defer listener.Close()

	log.Printf("Listening on port %s", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func main() {
	baseDir := "server"

	// 初始化全局变量
	HashStringMap = make(map[uint64]string)
	nodeTrees = make(map[string]*merkletree.MerkleTree)

	// 初始化分块管理器
	cm = chunk.NewChunkManager(baseDir)

	// 初始化 Fabric 客户端（可选，依赖环境变量/命令行参数）
	// 若初始化失败，不影响存储服务主流程，仅记录日志
	if fc, err := fabric.InitFabricClientFromFlags(); err != nil {
		log.Printf("Fabric client init failed: %v (Merkle root on-chain update disabled)", err)
	} else {
		fabricClient = fc
		defer fabricClient.Close()
		log.Printf("Fabric client initialized for on-chain Merkle updates")
	}

	// 读取防抖间隔（毫秒），默认 10000ms
	if msStr := os.Getenv("MERKLE_UPDATE_DEBOUNCE_MS"); msStr != "" {
		if v, err := strconv.Atoi(msStr); err == nil && v >= 0 {
			debounceInterval = time.Duration(v) * time.Millisecond
		} else {
			debounceInterval = 10 * time.Second
		}
	} else {
		debounceInterval = 10 * time.Second
	}
	nodeTimers = make(map[string]*time.Timer)

	// 初始化下载流水线管理器
	pipelineMgr = pipeline.NewPipeline(readHandler)

	// 设置证明处理器
	pipelineMgr.SetProofHandler(generateMerkleProof)

	pipelineMgr.Start()
	defer pipelineMgr.Stop()

	// 读取批量上链时间间隔（秒），默认 10 秒
	if intervalStr := os.Getenv("MERKLE_BATCH_UPDATE_INTERVAL"); intervalStr != "" {
		if v, err := strconv.Atoi(intervalStr); err == nil && v > 0 {
			batchInterval = time.Duration(v) * time.Second
		}
	}
	pendingMerkleRoots = make(map[string]bool)
	batchTicker = time.NewTicker(batchInterval)
	go func() {
		for range batchTicker.C {
			doBatchMerkleRoots()
		}
	}()

	// 启动多个端口监听
	ports := []string{"8082", "8080", "8081", "8083"}
	for _, port := range ports {
		go startServer(port)
	}

	/* 	// 计算所有节点的默克尔根
	   	results, err := merkleroot.CalculateAllNodesMerkleRoots(baseDir)
	   	if err != nil {
	   		log.Printf("Error calculating merkle roots: %v", err)
	   	} else {
	   		for nodeName, rootHash := range results {
	   			fmt.Printf("%s: %s\n", nodeName, rootHash)
	   		}
	   	} */

	// 阻塞主线程
	select {}
}
