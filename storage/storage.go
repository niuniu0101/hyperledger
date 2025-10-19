// storage.go (修改部分)
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	// "sync"

	"github.com/hyperledger/storage/chunk"
	"github.com/hyperledger/storage/merkleroot"
	"github.com/hyperledger/storage/pipeline"
)

// 全局变量
var (
	HashStringMap map[uint64]string
	cm            *chunk.ChunkManager
	// mu            sync.RWMutex
	pipelineMgr *pipeline.Pipeline // 下载流水线管理器
)

func handleConnection(conn net.Conn) {
	for {
		// 读取操作类型（1字节）：0-下载，1-上传
		opTypeBuffer := make([]byte, 1)
		n, err := conn.Read(opTypeBuffer)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client closed connection")
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("Connection timeout")
				return
			}
			log.Printf("Error reading operation type: %v", err)
			return
		}
		if n == 0 {
			log.Printf("Client disconnected")
			return
		}

		opType := opTypeBuffer[0]

		switch opType {
		case 0: // 下载操作
			handleDownload(conn)
		case 1: // 上传操作
			handleUpload(conn)
		default:
			log.Printf("Unknown operation type: %d", opType)
			conn.Write([]byte("ERROR: Unknown operation type"))
		}
	}
}

// 处理文件上传
func handleUpload(conn net.Conn) {
	// 读取文件名的哈希值（8B）
	fileHashBuffer := make([]byte, 8)
	if _, err := conn.Read(fileHashBuffer); err != nil {
		log.Printf("Error reading file hash: %v", err)
		conn.Write([]byte("ERROR: Failed to read file hash"))
		return
	}
	fileHash := binary.BigEndian.Uint64(fileHashBuffer)

	// 读取文件名（16B）
	fileNameBuffer := make([]byte, 16)
	if _, err := conn.Read(fileNameBuffer); err != nil {
		log.Printf("Error reading file name: %v", err)
		conn.Write([]byte("ERROR: Failed to read file name"))
		return
	}
	// 去除填充的空字节
	fileName := string(bytes.TrimRight(fileNameBuffer, "\x00"))

	// 读取节点名（8B）
	nodeNameBuffer := make([]byte, 8)
	if _, err := conn.Read(nodeNameBuffer); err != nil {
		log.Printf("Error reading node name: %v", err)
		conn.Write([]byte("ERROR: Failed to read node name"))
		return
	}
	nodeName := string(bytes.TrimRight(nodeNameBuffer, "\x00"))

	// 读取文件长度（8B）
	fileLengthBuffer := make([]byte, 8)
	if _, err := conn.Read(fileLengthBuffer); err != nil {
		log.Printf("Error reading file length: %v", err)
		conn.Write([]byte("ERROR: Failed to read file length"))
		return
	}
	fileLength := binary.BigEndian.Uint64(fileLengthBuffer)

	// 读取文件内容
	fileContent := make([]byte, fileLength)
	bytesRead := uint64(0)

	for bytesRead < fileLength {
		n, err := conn.Read(fileContent[bytesRead:])
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Error reading file content: %v", err)
			conn.Write([]byte("ERROR: Failed to read file content"))
			return
		}
		bytesRead += uint64(n)
	}

	if bytesRead != fileLength {
		log.Printf("File content size mismatch: expected %d, got %d", fileLength, bytesRead)
		conn.Write([]byte("ERROR: File content size mismatch"))
		return
	}

	// 同步写入文件到块存储 - 使用优化后的接口
	if err := cm.WriteObject(nodeName, fileName, fileContent); err != nil {
		log.Printf("Error writing object to chunk: %v", err)
		conn.Write([]byte("ERROR: " + err.Error()))
		return
	}

	// 更新哈希映射
	// mu.Lock()
	HashStringMap[fileHash] = fileName
	// mu.Unlock()

	// 发送成功响应
	response := []byte("SUCCESS: File uploaded successfully")
	if _, err := conn.Write(response); err != nil {
		log.Printf("Error sending success response: %v", err)
	} else {
		log.Printf("Successfully uploaded file %s to node %s (hash: %d, size: %d bytes)",
			fileName, nodeName, fileHash, fileLength)
	}
}

func handleDownload(conn net.Conn) {
	// 读取节点名称(8B)
	nodeNameBuffer := make([]byte, 8)
	n, err := conn.Read(nodeNameBuffer)
	if err != nil {
		log.Printf("Error reading node name: %v", err)
		return
	}
	nodeName := string(nodeNameBuffer[:n])

	// 读取文件哈希(8B)
	fileHashBuffer := make([]byte, 8)
	if _, err := conn.Read(fileHashBuffer); err != nil {
		log.Printf("Error reading file hash: %v", err)
		return
	}
	fileHash := binary.BigEndian.Uint64(fileHashBuffer)

	// 查找文件名
	// mu.RLock()
	fileName, exists := HashStringMap[fileHash]
	// mu.RUnlock()

	if !exists {
		log.Printf("File with hash %d not found", fileHash)
		conn.Write([]byte("00000000"))
		return
	}

	// 使用流水线处理下载请求 - 移除NodeName字段
	request := &pipeline.DownloadRequest{
		FileName: fileName, // 只保留FileName
		FileHash: fileHash,
		Conn:     conn,
	}

	if pipelineMgr.SubmitDownloadRequest(request) {
		log.Printf("Download request submitted to pipeline: %s (node: %s)", fileName, nodeName)
	} else {
		// 如果流水线繁忙，回退到同步处理
		log.Printf("Pipeline busy, falling back to synchronous download for: %s (node: %s)", fileName, nodeName)
		handleDownloadSync(conn, fileName, fileHash)
	}
}

// sendObject 发送对象内容到客户端
func sendObject(conn net.Conn, value []byte, fileName string, fileHash uint64) error {
	// 发送文件哈希 (8字节)
	hashBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashBytes, fileHash)
	if _, err := conn.Write(hashBytes); err != nil {
		return fmt.Errorf("failed to send file hash: %v", err)
	}

	// 发送文件长度 (8字节)
	lengthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(value)))
	if _, err := conn.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to send file length: %v", err)
	}

	// 发送文件内容
	if _, err := conn.Write(value); err != nil {
		return fmt.Errorf("failed to send file content: %v", err)
	}

	log.Printf("Successfully sent object %s (size: %d bytes, hash: %d)",
		fileName, len(value), fileHash)
	return nil
}

// 回退下载 - 简化参数，不再需要nodeName
func handleDownloadSync(conn net.Conn, fileName string, fileHash uint64) {
	// defer conn.Close()

	// 从块中读取对象 - 使用优化后的接口，只需要fileName
	value, err := cm.ReadObject(fileName)
	if err != nil {
		log.Printf("Error reading object from chunk: %v", err)
		conn.Write([]byte("ERROR: " + err.Error()))
		return
	}

	// 发送对象内容
	if err := sendObject(conn, value, fileName, fileHash); err != nil {
		log.Printf("Error sending object: %v", err)
	} else {
		log.Printf("Successfully sent object %s via sync download", fileName)
	}
}

// 读取处理器 - 供流水线调用 - 简化参数
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
	// defer listener.Close()

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

	// 初始化全局索引
	HashStringMap = make(map[uint64]string)

	// 初始化分块管理器
	cm = chunk.NewChunkManager(baseDir)

	// 初始化下载流水线管理器 - 使用简化后的readHandler
	pipelineMgr = pipeline.NewPipeline(readHandler)
	pipelineMgr.Start()
	defer pipelineMgr.Stop()

	// 启动多个端口监听
	ports := []string{"8082"}
	for _, port := range ports {
		go startServer(port)
	}

	// server/下的每一个node对应一棵默克尔树
	results, err := merkleroot.CalculateAllNodesMerkleRoots(baseDir)
	if err != nil {
		log.Printf("Error calculating merkle roots: %v", err)
	} else {
		for nodeName, rootHash := range results {
			fmt.Printf("%s: %s\n", nodeName, rootHash)
		}
	}

	/* 	// 获取统计信息
	   	objCount, chunkCount, stripeCount := cm.GetStats()
	   	log.Printf("Server initialized with %d objects, %d chunks, %d stripes",
	   		objCount, chunkCount, stripeCount)

	   	// 获取环统计信息
	   	ringStats := cm.GetRingStats()
	   	for i, stats := range ringStats {
	   		log.Printf("Ring %d: Active=%d, Inactive=%d, TotalSize=%d",
	   			i, stats.ActiveChunks, stats.InactiveChunks, stats.TotalSize)
	   	}

	   	// 获取流水线状态
	   	pipelineStats := pipelineMgr.GetStats()
	   	log.Printf("Download pipeline stats: %+v", pipelineStats) */

	// 阻塞主线程
	select {}
}
