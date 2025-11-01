package chunk

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

const (
	ChunkSize        = 256 * 1024 // 4KiB
	NumDataRings     = 4          // 数据环
	NumParityRings   = 2          // 校验环
	TotalDataNodes   = 40         // node0-node39
	TotalParityNodes = 20         // node40-node59
	MinChunksForEC   = 4          // 触发EC的最小非活跃chunk数
)

// 服务器配置
var serverConfigs = map[uint8][]string{

	0: {"10.0.0.184:8082"},    // 环0: node0,node4,node8...node36
	1: {"localhost:8082"},     // 环1: node1,node5,node9...node37
	2: {"10.0.0.186:8082"},    // 环2: node2,node6,node10...node38
	3: {"10.0.0.187:8082"},    // 环3: node3,node7,node11...node39
	4: {"8.220.202.165:8082"}, // 环4: node40-node49
	5: {"8.220.202.165:8082"}, // 环5: node50-node59
}

/*
	var ringToServer = map[int]string{
		0: "localhost:8082",
		1: "10.0.0.185:8082",
		4: "localhost:8082",
		5: "localhost:8082",
		2: "10.0.0.186:8082",
		3: "10.0.0.187:8082",
	}
*/
type ObjectMeta struct {
	Length    uint32
	Offset    uint32
	StripeID  uint32
	ChunkID   uint32
	RingIndex uint8
}

type ChunkMeta struct {
	ChunkID       uint32
	ObjectKeys    []string
	Size          uint32
	IsActive      bool
	IsDistributed bool // 标记是否已分发
	NodeName      string
	RingIndex     uint8
	FilePath      string
}

type StripeMeta struct {
	StripeID       uint32
	DataChunkIDs   []uint32
	ParityChunkIDs []uint32
	RingIndices    []uint8
}

type RingStats struct {
	ActiveChunks   int
	InactiveChunks int
	TotalSize      uint64
}

// 连接管理器
type ConnectionManager struct {
	connections map[string]net.Conn
	mu          sync.RWMutex
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]net.Conn),
	}
}

func (cm *ConnectionManager) GetConnection(serverAddr string) (net.Conn, error) {
	cm.mu.RLock()
	conn, exists := cm.connections[serverAddr]
	cm.mu.RUnlock()

	if exists && conn != nil {
		/* // 测试连接是否仍然有效
		if err := cm.testConnection(conn); err == nil {
			return conn, nil
		}
		// 连接无效，关闭并移除
		conn.Close()
		cm.mu.Lock()
		delete(cm.connections, serverAddr)
		cm.mu.Unlock() */
		return conn, nil
	}

	// 创建新连接
	newConn, err := net.DialTimeout("tcp", serverAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial server %s: %v", serverAddr, err)
	}

	// 设置连接选项
	newConn.SetDeadline(time.Now().Add(30 * time.Minute)) // 设置较长的超时时间

	cm.mu.Lock()
	cm.connections[serverAddr] = newConn
	cm.mu.Unlock()

	return newConn, nil
}

/* func (cm *ConnectionManager) testConnection(conn net.Conn) error {
	// 简单的连接测试：尝试设置一个很短的读取超时，然后尝试读取一个字节（应该会超时）
	conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	defer conn.SetReadDeadline(time.Time{}) // 重置超时

	_, err := conn.Read(make([]byte, 1))
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// 超时是预期的，说明连接仍然活跃
			return nil
		}
		// 其他错误说明连接可能已经关闭
		return err
	}
	return nil
} */

func (cm *ConnectionManager) CloseConnection(serverAddr string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if conn, exists := cm.connections[serverAddr]; exists {
		conn.Close()
		delete(cm.connections, serverAddr)
	}
}

func (cm *ConnectionManager) CloseAll() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for addr, conn := range cm.connections {
		conn.Close()
		delete(cm.connections, addr)
	}
}

type ChunkManager struct {
	baseDir      string
	activeChunks map[string]*ChunkMeta
	nodeRings    map[string]uint8

	objectIndex map[string]*ObjectMeta
	chunkIndex  map[uint32]*ChunkMeta
	stripeIndex map[uint32]*StripeMeta
	mu          sync.RWMutex

	nextChunkID  uint32
	nextStripeID uint32
	ringStats    [NumDataRings + NumParityRings]*RingStats
	encoder      reedsolomon.Encoder
	connManager  *ConnectionManager
}

func NewChunkManager(baseDir string) *ChunkManager {
	enc, err := reedsolomon.New(NumDataRings, NumParityRings)
	if err != nil {
		log.Fatalf("Failed to create encoder: %v", err)
	}

	cm := &ChunkManager{
		baseDir:      baseDir,
		activeChunks: make(map[string]*ChunkMeta),
		nodeRings:    make(map[string]uint8),
		objectIndex:  make(map[string]*ObjectMeta),
		chunkIndex:   make(map[uint32]*ChunkMeta),
		stripeIndex:  make(map[uint32]*StripeMeta),
		encoder:      enc,
		connManager:  NewConnectionManager(),
	}

	for i := range cm.ringStats {
		cm.ringStats[i] = &RingStats{}
	}

	cm.initializeNodeRings()
	return cm
}

func (cm *ChunkManager) initializeNodeRings() {
	for i := 0; i < TotalDataNodes+TotalParityNodes; i++ {
		nodeName := fmt.Sprintf("node%d", i)
		nodeDir := filepath.Join(cm.baseDir, nodeName)

		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			log.Printf("Warning: failed to create node dir %s: %v", nodeDir, err)
			continue
		}

		ringIndex := cm.calculateRingIndex(i)
		cm.nodeRings[nodeName] = ringIndex
		log.Printf("Node %s -> ring %d", nodeName, ringIndex)
	}
}

func (cm *ChunkManager) calculateRingIndex(nodeNum int) uint8 {
	if nodeNum < TotalDataNodes {
		return uint8(nodeNum % NumDataRings)
	}
	parityNodeIndex := nodeNum - TotalDataNodes
	return uint8(NumDataRings + (parityNodeIndex % NumParityRings))
}

func (cm *ChunkManager) getRingForNode(nodeName string) (uint8, error) {
	if ringIndex, exists := cm.nodeRings[nodeName]; exists {
		return ringIndex, nil
	}

	// 动态解析节点名称
	if strings.HasPrefix(nodeName, "node") {
		nodeNumStr := strings.TrimPrefix(nodeName, "node")
		if nodeNum, err := strconv.Atoi(nodeNumStr); err == nil {
			if nodeNum >= 0 && nodeNum < TotalDataNodes+TotalParityNodes {
				ringIndex := cm.calculateRingIndex(nodeNum)
				cm.nodeRings[nodeName] = ringIndex
				return ringIndex, nil
			}
		}
	}

	return 0, fmt.Errorf("node %s not found", nodeName)
}

func (cm *ChunkManager) updateRingStats(chunk *ChunkMeta, isNew bool) {
	if int(chunk.RingIndex) >= len(cm.ringStats) {
		return
	}

	stats := cm.ringStats[chunk.RingIndex]
	if isNew {
		if chunk.IsActive {
			stats.ActiveChunks++
		} else {
			stats.InactiveChunks++
		}
		stats.TotalSize += uint64(chunk.Size)
	} else {
		if chunk.IsActive {
			stats.InactiveChunks--
			stats.ActiveChunks++
		} else {
			stats.ActiveChunks--
			stats.InactiveChunks++
		}
	}
}

// 统一的chunk创建逻辑
func (cm *ChunkManager) createChunk(nodeName string, isActive bool, prefix string) (*ChunkMeta, error) {
	ringIndex, err := cm.getRingForNode(nodeName)
	if err != nil {
		return nil, err
	}

	nodeDir := filepath.Join(cm.baseDir, nodeName)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, fmt.Errorf("create node dir: %v", err)
	}

	chunkID := cm.nextChunkID
	cm.nextChunkID++

	filename := fmt.Sprintf("%s_chunk_%d.dat", prefix, chunkID)
	chunkPath := filepath.Join(nodeDir, filename)

	if err := os.WriteFile(chunkPath, []byte{}, 0644); err != nil {
		return nil, fmt.Errorf("create chunk file: %v", err)
	}

	chunk := &ChunkMeta{
		ChunkID:    chunkID,
		NodeName:   nodeName,
		RingIndex:  ringIndex,
		IsActive:   isActive,
		Size:       0,
		FilePath:   chunkPath,
		ObjectKeys: make([]string, 0),
	}

	if isActive {
		cm.activeChunks[nodeName] = chunk
	}
	cm.chunkIndex[chunkID] = chunk
	cm.updateRingStats(chunk, true)

	log.Printf("Created %s chunk %d for node %s (ring %d)", prefix, chunkID, nodeName, ringIndex)
	return chunk, nil
}

/*
func (cm *ChunkManager) getActiveChunk(nodeName string) (*ChunkMeta, error) {
	return cm.getOrCreateActiveChunk(nodeName)
} */

func (cm *ChunkManager) getOrCreateActiveChunk(nodeName string) (*ChunkMeta, error) {
	// 检查现有活跃chunk
	if chunk, exists := cm.activeChunks[nodeName]; exists && chunk.IsActive {
		if chunk.Size < ChunkSize {
			return chunk, nil
		}
		// chunk已满，标记为非活跃
		chunk.IsActive = false
		cm.updateRingStats(chunk, false)
		cm.checkAndTriggerEC()
	}

	// 创建新chunk
	return cm.createChunk(nodeName, true, "")
}

func (cm *ChunkManager) checkAndTriggerEC() {
	/* 	cm.mu.Lock()
	   	defer cm.mu.Unlock() */

	log.Printf("Checking EC condition...")

	// 检查所有数据环都有未分发的非活跃chunk
	for i := 0; i < NumDataRings; i++ {
		hasUndistributed := false
		for _, chunk := range cm.chunkIndex {
			if uint8(i) == chunk.RingIndex && !chunk.IsActive && !chunk.IsDistributed {
				if _, err := os.Stat(chunk.FilePath); err == nil {
					hasUndistributed = true
					break
				}
			}
		}
		if !hasUndistributed {
			log.Printf("EC condition not met: ring %d has no undistributed inactive chunks", i)
			return
		}
	}

	log.Printf("All rings have undistributed inactive chunks, triggering EC...")
	cm.performErasureCoding()
}

func (cm *ChunkManager) performErasureCoding() {
	dataChunks := cm.collectInactiveDataChunks()
	if len(dataChunks) != NumDataRings {
		log.Printf("Not enough inactive chunks: expected %d, got %d", NumDataRings, len(dataChunks))
		return
	}

	shards, err := cm.readAndPrepareShards(dataChunks)
	if err != nil {
		log.Printf("Error preparing shards: %v", err)
		return
	}

	if err := cm.encoder.Encode(shards); err != nil {
		log.Printf("Error encoding: %v", err)
		return
	}

	parityChunkIDs := cm.createParityChunks(shards[NumDataRings:])
	cm.createStripeMetadata(getChunkIDs(dataChunks), parityChunkIDs)
	cm.updateChunkStats(dataChunks)

	// 分发chunk到远程服务器并清理本地文件
	cm.distributeAndCleanup(dataChunks, cm.getParityChunks(parityChunkIDs))

	log.Printf("EC completed: created parity chunks %v", parityChunkIDs)
}

func (cm *ChunkManager) collectInactiveDataChunks() []*ChunkMeta {
	var chunks []*ChunkMeta
	usedChunks := make(map[uint32]bool) // 防止重复选择

	for ringIndex := 0; ringIndex < NumDataRings; ringIndex++ {
		found := false
		for _, chunk := range cm.chunkIndex {
			// 检查条件：匹配环、非活跃、未分发、文件存在、未使用过
			if uint8(ringIndex) == chunk.RingIndex &&
				!chunk.IsActive &&
				!chunk.IsDistributed &&
				!usedChunks[chunk.ChunkID] {

				if _, err := os.Stat(chunk.FilePath); os.IsNotExist(err) {
					log.Printf("Chunk %d file not found, skipping", chunk.ChunkID)
					continue
				}

				chunks = append(chunks, chunk)
				usedChunks[chunk.ChunkID] = true
				log.Printf("Selected chunk %d from ring %d for EC", chunk.ChunkID, ringIndex)
				found = true
				break
			}
		}
		if !found {
			log.Printf("No available undistributed inactive chunk found for ring %d", ringIndex)
		}
	}
	return chunks
}

func (cm *ChunkManager) readAndPrepareShards(dataChunks []*ChunkMeta) ([][]byte, error) {
	shards := make([][]byte, NumDataRings+NumParityRings)
	maxSize := cm.getMaxChunkSize(dataChunks)

	// 读取数据分片
	for i, chunk := range dataChunks {
		// 检查文件是否存在
		if _, err := os.Stat(chunk.FilePath); os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk file %s does not exist", chunk.FilePath)
		}

		content, err := os.ReadFile(chunk.FilePath)
		if err != nil {
			return nil, fmt.Errorf("read chunk %d: %v", chunk.ChunkID, err)
		}

		shard := make([]byte, maxSize)
		copy(shard, content)
		shards[i] = shard
	}

	// 准备校验分片内存
	for i := NumDataRings; i < NumDataRings+NumParityRings; i++ {
		shards[i] = make([]byte, maxSize)
	}

	return shards, nil
}

func (cm *ChunkManager) getMaxChunkSize(chunks []*ChunkMeta) int {
	maxSize := 0
	for _, chunk := range chunks {
		if info, err := os.Stat(chunk.FilePath); err == nil {
			if size := int(info.Size()); size > maxSize {
				maxSize = size
			}
		}
	}
	return maxSize
}

func (cm *ChunkManager) createParityChunks(parityShards [][]byte) []uint32 {
	var parityChunkIDs []uint32

	for i, shard := range parityShards {
		parityNodeIndex := TotalDataNodes + (i%(TotalParityNodes/NumParityRings))*NumParityRings + i
		if parityNodeIndex >= TotalDataNodes+TotalParityNodes {
			parityNodeIndex = TotalDataNodes + i
		}

		nodeName := fmt.Sprintf("node%d", parityNodeIndex)
		chunk, err := cm.createChunk(nodeName, false, "parity")
		if err != nil {
			log.Printf("Error creating parity chunk: %v", err)
			continue
		}

		if err := os.WriteFile(chunk.FilePath, shard, 0644); err != nil {
			log.Printf("Error writing parity chunk: %v", err)
			continue
		}

		chunk.Size = uint32(len(shard))
		parityChunkIDs = append(parityChunkIDs, chunk.ChunkID)
	}

	return parityChunkIDs
}

func (cm *ChunkManager) getParityChunks(parityChunkIDs []uint32) []*ChunkMeta {
	var chunks []*ChunkMeta
	for _, id := range parityChunkIDs {
		if chunk, exists := cm.chunkIndex[id]; exists {
			chunks = append(chunks, chunk)
		}
	}
	return chunks
}

func (cm *ChunkManager) createStripeMetadata(dataChunkIDs, parityChunkIDs []uint32) {
	stripeID := cm.nextStripeID
	cm.nextStripeID++

	stripe := &StripeMeta{
		StripeID:       stripeID,
		DataChunkIDs:   dataChunkIDs,
		ParityChunkIDs: parityChunkIDs,
		RingIndices:    make([]uint8, NumDataRings+NumParityRings),
	}

	for i := 0; i < NumDataRings+NumParityRings; i++ {
		if i < NumDataRings {
			stripe.RingIndices[i] = uint8(i)
		} else {
			stripe.RingIndices[i] = uint8(NumDataRings + (i - NumDataRings))
		}
	}

	cm.stripeIndex[stripeID] = stripe
}

func (cm *ChunkManager) updateChunkStats(dataChunks []*ChunkMeta) {
	for _, chunk := range dataChunks {
		cm.ringStats[chunk.RingIndex].InactiveChunks--
	}
}

func (cm *ChunkManager) distributeAndCleanup(dataChunks []*ChunkMeta, parityChunks []*ChunkMeta) {
	allChunks := append([]*ChunkMeta{}, dataChunks...)
	// allChunks = append(allChunks, parityChunks...)   暂时不分发校验块

	log.Printf("Starting sequential distribution: %d chunks total", len(dataChunks))

	// 按环分组，为每个环创建一个连接
	ringConnections := make(map[uint8]net.Conn)
	/* 	defer func() {
		// 清理所有连接
		for _, conn := range ringConnections {
			conn.Close()
		}
	}() */

	// 首先为所有需要的环建立连接
	for _, chunk := range allChunks {
		if cm.shouldStoreLocally(chunk) {
			continue
		}

		if _, exists := ringConnections[chunk.RingIndex]; !exists {
			servers, ok := serverConfigs[chunk.RingIndex]
			if !ok || len(servers) == 0 {
				log.Printf("No server configured for ring %d", chunk.RingIndex)
				continue
			}

			serverAddr := servers[0]
			conn, err := cm.connManager.GetConnection(serverAddr)
			if err != nil {
				log.Printf("Failed to create connection for ring %d: %v", chunk.RingIndex, err)
				continue
			}
			ringConnections[chunk.RingIndex] = conn
			log.Printf("Created connection for ring %d to server %s", chunk.RingIndex, serverAddr)
		}
	}

	// // 顺序处理每个chunk
	// for i, chunk := range allChunks {
	// 	log.Printf("[%d/%d] Processing chunk %d (node: %s, ring: %d, objects: %d)",
	// 		i+1, len(allChunks), chunk.ChunkID, chunk.NodeName, chunk.RingIndex, len(chunk.ObjectKeys))

	// 	// 检查这个chunk是否应该存储在本地服务器
	// 	if cm.shouldStoreLocally(chunk) {
	// 		log.Printf("Chunk %d belongs to local server, skipping transfer", chunk.ChunkID)
	// 		continue
	// 	}

	// 	// 获取该环的连接
	// 	conn, exists := ringConnections[chunk.RingIndex]
	// 	if !exists {
	// 		log.Printf("No connection available for ring %d, skipping chunk %d", chunk.RingIndex, chunk.ChunkID)
	// 		continue
	// 	}

	// 	// 发送chunk中的所有小文件
	// 	log.Printf("Chunk %d: starting transfer of %d objects", chunk.ChunkID, len(chunk.ObjectKeys))
	// 	if err := cm.sendChunkFilesWithConnection(conn, chunk); err != nil {
	// 		log.Printf("Failed to send chunk %d files to server: %v", chunk.ChunkID, err)
	// 		continue
	// 	}

	// 	// 发送成功后删除本地chunk文件
	// 	// if err := os.Remove(chunk.FilePath); err != nil {
	// 	// 	log.Printf("Failed to remove local chunk file %s: %v", chunk.FilePath, err)
	// 	// } else {
	// 	// 	log.Printf("Successfully removed local chunk file: %s", chunk.FilePath)
	// 	// }

	// 	log.Printf("Chunk %d: processing completed", chunk.ChunkID)
	// }
	for i, chunk := range allChunks {
		log.Printf("[%d/%d] Processing chunk %d (node: %s, ring: %d, objects: %d)",
			i+1, len(allChunks), chunk.ChunkID, chunk.NodeName, chunk.RingIndex, len(chunk.ObjectKeys))

		if cm.shouldStoreLocally(chunk) {
			log.Printf("Chunk %d belongs to local server, skipping transfer", chunk.ChunkID)
			// 即使是本地chunk，也标记为已分发，避免重复处理
			cm.markChunkAsDistributed(chunk)
			continue
		}

		// 获取该环的连接
		conn, exists := ringConnections[chunk.RingIndex]
		if !exists {
			log.Printf("No connection available for ring %d, skipping chunk %d", chunk.RingIndex, chunk.ChunkID)
			continue
		}

		// 发送chunk中的所有小文件
		log.Printf("Chunk %d: starting transfer of %d objects", chunk.ChunkID, len(chunk.ObjectKeys))
		if err := cm.sendChunkFilesWithConnection(conn, chunk); err != nil {
			log.Printf("Failed to send chunk %d files to server: %v", chunk.ChunkID, err)
			continue
		}

		// 发送成功后标记为已分发
		cm.markChunkAsDistributed(chunk)

		log.Printf("Chunk %d: processing completed", chunk.ChunkID)
	}
	log.Printf("All chunks processed sequentially")
}

// 新增方法：标记chunk为已分发
func (cm *ChunkManager) markChunkAsDistributed(chunk *ChunkMeta) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if existingChunk, exists := cm.chunkIndex[chunk.ChunkID]; exists {
		existingChunk.IsDistributed = true
		// 更新统计信息
		if !existingChunk.IsActive {
			cm.ringStats[chunk.RingIndex].InactiveChunks--
		}
		log.Printf("Marked chunk %d as distributed (ring %d)", chunk.ChunkID, chunk.RingIndex)
	}
}

// sendChunkFilesWithConnection 使用已建立的连接发送chunk中的文件
func (cm *ChunkManager) sendChunkFilesWithConnection(conn net.Conn, chunk *ChunkMeta) error {
	// 读取chunk文件内容
	log.Printf("Chunk %d: reading chunk file %s", chunk.ChunkID, chunk.FilePath)
	chunkContent, err := os.ReadFile(chunk.FilePath)
	if err != nil {
		return fmt.Errorf("read chunk file: %v", err)
	}
	log.Printf("Chunk %d: read %d bytes from file", chunk.ChunkID, len(chunkContent))

	// 发送chunk中的每个小文件
	successCount := 0
	for i, objectKey := range chunk.ObjectKeys {
		log.Printf("Chunk %d: sending object %d/%d - %s",
			chunk.ChunkID, i+1, len(chunk.ObjectKeys), objectKey)

		if err := cm.sendSingleFileWithConnection(conn, chunk, objectKey, chunkContent); err != nil {
			log.Printf("Failed to send file %s from chunk %d: %v", objectKey, chunk.ChunkID, err)
			// 继续尝试发送其他文件
			continue
		}
		successCount++
		log.Printf("Chunk %d: successfully sent object %d/%d", chunk.ChunkID, i+1, len(chunk.ObjectKeys))
	}

	log.Printf("Chunk %d: transfer completed - %d/%d objects sent successfully",
		chunk.ChunkID, successCount, len(chunk.ObjectKeys))

	if successCount == 0 {
		return fmt.Errorf("no objects were successfully transferred")
	}

	return nil
}

// sendSingleFileWithConnection 修复版本
func (cm *ChunkManager) sendSingleFileWithConnection(conn net.Conn, chunk *ChunkMeta, objectKey string, chunkContent []byte) error {
	// 打开日志文件
	logFile, err := os.OpenFile("./chunk_transfer.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer logFile.Close()

	logger := log.New(logFile, "", log.LstdFlags)
	logger.Printf("Starting transfer for object %s from chunk %d", objectKey, chunk.ChunkID)

	// 获取对象元数据
	objectMeta, exists := cm.objectIndex[objectKey]
	if !exists {
		logger.Printf("ERROR: Object meta not found for key: %s", objectKey)
		return fmt.Errorf("object meta not found for key: %s", objectKey)
	}

	logger.Printf("Object meta: offset=%d, length=%d, chunkSize=%d",
		objectMeta.Offset, objectMeta.Length, len(chunkContent))

	// 验证数据边界
	if int(objectMeta.Offset) >= len(chunkContent) {
		errorMsg := fmt.Sprintf("object offset beyond chunk bounds: offset=%d, chunkSize=%d",
			objectMeta.Offset, len(chunkContent))
		logger.Printf("ERROR: %s", errorMsg)
		return fmt.Errorf(errorMsg)
	}

	if int(objectMeta.Offset+objectMeta.Length) > len(chunkContent) {
		errorMsg := fmt.Sprintf("object data exceeds chunk bounds: offset=%d, length=%d, chunkSize=%d",
			objectMeta.Offset, objectMeta.Length, len(chunkContent))
		logger.Printf("ERROR: %s", errorMsg)

		// 尝试读取到文件末尾
		available := len(chunkContent) - int(objectMeta.Offset)
		if available > 0 {
			logger.Printf("WARNING: Truncating object data from %d to %d bytes",
				objectMeta.Length, available)
			objectMeta.Length = uint32(available)
		} else {
			return fmt.Errorf(errorMsg)
		}
	}

	// 提取对象数据
	start := int(objectMeta.Offset)
	end := start + int(objectMeta.Length)
	objectData := chunkContent[start:end]

	logger.Printf("Extracted object data: %d bytes", len(objectData))

	// 准备上传报文
	message := cm.prepareFileUploadMessage(objectKey, chunk.NodeName, objectData)
	logger.Printf("Prepared upload message: %d bytes total", len(message))

	// 设置写超时
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	// 分块发送数据，避免大报文问题
	chunkSize := 64 * 1024 // 64KB 分块
	totalSent := 0

	for totalSent < len(message) {
		end := totalSent + chunkSize
		if end > len(message) {
			end = len(message)
		}

		chunkData := message[totalSent:end]
		n, err := conn.Write(chunkData)
		if err != nil {
			logger.Printf("ERROR: Failed to write chunk data: %v", err)
			return fmt.Errorf("write chunk data: %v", err)
		}

		totalSent += n
		logger.Printf("Progress: sent %d/%d bytes (%.1f%%)",
			totalSent, len(message), float64(totalSent)*100/float64(len(message)))
	}

	logger.Printf("Successfully sent entire message: %d bytes", totalSent)

	// 等待服务器响应
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	response := make([]byte, 1)
	_, err = conn.Read(response)
	if err != nil {
		logger.Printf("WARNING: No response from server: %v", err)
		// 不立即返回错误，可能服务器不发送响应
	} else {
		logger.Printf("Received server response: %v", response[0])
	}

	logger.Printf("Completed transfer for object %s", objectKey)
	return nil
}

/* func writeAll(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := conn.Write(data[totalWritten:])
		if err != nil {
			return fmt.Errorf("写入失败: %w", err)
		}
		totalWritten += n
		log.Printf("已写入 %d/%d 字节", totalWritten, len(data))
	}
	return nil
} */

// prepareFileUploadMessage 准备单个文件的上传报文
func (cm *ChunkManager) prepareFileUploadMessage(objectKey, nodeName string, content []byte) []byte {
	// 1B: 操作类型 (1=上传, 0=下载)
	operation := byte(1)

	// 2-9B: 文件名哈希 (使用objectKey计算哈希)
	fileNameHash := make([]byte, 8)
	hash := sha256Hash64(objectKey)
	binary.BigEndian.PutUint64(fileNameHash, hash)

	// 10-25B: 文件名 (固定16字节，不足补0)
	fileNameBytes := make([]byte, 16)
	copy(fileNameBytes, objectKey)

	// 26-33B: 节点名 (固定8字节，不足补0)
	nodeNameBytes := make([]byte, 8)
	copy(nodeNameBytes, nodeName)

	// 34-41B: 文件长度 (8字节)
	fileSize := make([]byte, 8)
	binary.BigEndian.PutUint64(fileSize, uint64(len(content)))

	// 构建完整报文
	message := make([]byte, 0, 41+len(content))
	message = append(message, operation)
	message = append(message, fileNameHash...)
	message = append(message, fileNameBytes...)
	message = append(message, nodeNameBytes...)
	message = append(message, fileSize...)
	message = append(message, content...)

	return message
}

// shouldStoreLocally 检查chunk是否应该存储在本地服务器
func (cm *ChunkManager) shouldStoreLocally(chunk *ChunkMeta) bool {
	// 获取当前服务器的环索引
	currentServerRingIndex := uint8(1) // 当前服务器47.237.16.182对应环1

	// 如果chunk的环索引与当前服务器环索引相同，则应该存储在本地
	return chunk.RingIndex == currentServerRingIndex
}

func getChunkIDs(chunks []*ChunkMeta) []uint32 {
	ids := make([]uint32, len(chunks))
	for i, chunk := range chunks {
		ids[i] = chunk.ChunkID
	}
	return ids
}

// WriteObject 写入对象（简化版）
func (cm *ChunkManager) WriteObject(nodeName, key string, value []byte) error {
	chunk, err := cm.getOrCreateActiveChunk(nodeName)
	if err != nil {
		return err
	}

	// 检查空间
	if chunk.Size+uint32(len(value)) > ChunkSize {
		chunk.IsActive = false
		cm.updateRingStats(chunk, false)
		cm.checkAndTriggerEC()

		chunk, err = cm.createChunk(nodeName, true, "")
		if err != nil {
			return err
		}
	}

	// 写入文件
	file, err := os.OpenFile(chunk.FilePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open chunk file: %v", err)
	}
	defer file.Close()

	if _, err := file.Write(value); err != nil {
		return fmt.Errorf("write to chunk: %v", err)
	}

	// 更新元数据
	cm.objectIndex[key] = &ObjectMeta{
		Length:    uint32(len(value)),
		Offset:    chunk.Size,
		ChunkID:   chunk.ChunkID,
		RingIndex: chunk.RingIndex,
	}

	chunk.ObjectKeys = append(chunk.ObjectKeys, key)
	chunk.Size += uint32(len(value))

	return nil
}

// ReadObject 读取对象（简化版）
func (cm *ChunkManager) ReadObject(key string) ([]byte, error) {
	meta, exists := cm.objectIndex[key]
	if !exists {
		return nil, fmt.Errorf("object not found: %s", key)
	}

	chunk, exists := cm.chunkIndex[meta.ChunkID]
	if !exists {
		return nil, fmt.Errorf("chunk %d not found", meta.ChunkID)
	}

	// 读取文件（无锁）
	file, err := os.Open(chunk.FilePath)
	if err != nil {
		return nil, fmt.Errorf("open chunk file: %v", err)
	}
	defer file.Close()

	value := make([]byte, meta.Length)
	n, err := file.ReadAt(value, int64(meta.Offset))
	if err != nil {
		return nil, fmt.Errorf("read from chunk: %v", err)
	}

	if uint32(n) != meta.Length {
		return nil, fmt.Errorf("length mismatch: expected %d, got %d", meta.Length, n)
	}

	return value, nil
}

// GetNodeObjects 获取节点下的所有对象
func (cm *ChunkManager) GetNodeObjects(nodeName string) (map[string][]byte, error) {
	objects := make(map[string][]byte)

	// 遍历对象索引，找到属于该节点的所有对象
	for objectKey, objectMeta := range cm.objectIndex {
		chunk, exists := cm.chunkIndex[objectMeta.ChunkID]
		if !exists {
			continue
		}

		// 检查对象是否属于该节点
		if chunk.NodeName == nodeName {
			// 读取对象数据
			data, err := cm.ReadObject(objectKey)
			if err != nil {
				log.Printf("Warning: failed to read object %s: %v", objectKey, err)
				continue
			}
			objects[objectKey] = data
		}
	}

	log.Printf("Found %d objects for node %s", len(objects), nodeName)
	return objects, nil
}

// GetObjectKeysByNode 获取节点下的所有对象key
func (cm *ChunkManager) GetObjectKeysByNode(nodeName string) ([]string, error) {
	var objectKeys []string

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for objectKey, objectMeta := range cm.objectIndex {
		chunk, exists := cm.chunkIndex[objectMeta.ChunkID]
		if exists && chunk.NodeName == nodeName {
			objectKeys = append(objectKeys, objectKey)
		}
	}

	return objectKeys, nil
}

// GetObjectMeta 获取对象元数据
func (cm *ChunkManager) GetObjectMeta(objectKey string) (*ObjectMeta, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	meta, exists := cm.objectIndex[objectKey]
	return meta, exists
}

// 其他方法保持简洁
func (cm *ChunkManager) GetRingStats() []*RingStats {
	stats := make([]*RingStats, len(cm.ringStats))
	for i, s := range cm.ringStats {
		stats[i] = &RingStats{ActiveChunks: s.ActiveChunks, InactiveChunks: s.InactiveChunks, TotalSize: s.TotalSize}
	}
	return stats
}

func (cm *ChunkManager) GetStripeInfo(stripeID uint32) (*StripeMeta, error) {
	stripe, exists := cm.stripeIndex[stripeID]
	if !exists {
		return nil, fmt.Errorf("stripe %d not found", stripeID)
	}
	return stripe, nil
}

func (cm *ChunkManager) GetStats() (objects, chunks, stripes int) {
	return len(cm.objectIndex), len(cm.chunkIndex), len(cm.stripeIndex)
}

// 关闭ChunkManager，清理资源
func (cm *ChunkManager) Close() {
	cm.connManager.CloseAll()
}

// sha256Hash64 计算字符串的SHA256哈希，返回前8字节作为uint64
func sha256Hash64(s string) uint64 {
	// 计算SHA256哈希
	hash := sha256.Sum256([]byte(s))

	// 取前8字节转换为uint64
	// 使用大端序
	return binary.BigEndian.Uint64(hash[:8])
}
