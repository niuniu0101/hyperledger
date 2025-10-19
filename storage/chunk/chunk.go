package chunk

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const (
	ChunkSize        = 256 * 1024 // 4KiB
	NumDataRings     = 4        // 数据环
	NumParityRings   = 2        // 校验环
	TotalDataNodes   = 40       // node0-node39
	TotalParityNodes = 20       // node40-node59
	MinChunksForEC   = 4        // 触发EC的最小非活跃chunk数
)

type ObjectMeta struct {
	Length    uint32
	Offset    uint32
	StripeID  uint32
	ChunkID   uint32
	RingIndex uint8
}

type ChunkMeta struct {
	ChunkID    uint32
	ObjectKeys []string
	Size       uint32
	IsActive   bool
	NodeName   string
	RingIndex  uint8
	FilePath   string
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
	}

	for i := range cm.ringStats {
		cm.ringStats[i] = &RingStats{}
	}

	cm.initializeNodeRings()
	return cm
}

func (cm *ChunkManager) initializeNodeRings() {
	for i := 0; i < TotalDataNodes; i++ {

		if i%4 != 0 {
			continue
		}

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

func (cm *ChunkManager) getActiveChunk(nodeName string) (*ChunkMeta, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.getOrCreateActiveChunk(nodeName)
}

func (cm *ChunkManager) getOrCreateActiveChunk(nodeName string) (*ChunkMeta, error) {
	// 检查现有活跃chunk
	if chunk, exists := cm.activeChunks[nodeName]; exists && chunk.IsActive {
		if chunk.Size < ChunkSize {
			return chunk, nil
		}
		// chunk已满，标记为非活跃
		chunk.IsActive = false
		cm.updateRingStats(chunk, false)
		//cm.checkAndTriggerEC()
	}

	// 创建新chunk
	return cm.createChunk(nodeName, true, "")
}

// func (cm *ChunkManager) checkAndTriggerEC() {
// 	log.Printf("Checking EC condition...")
// 	for i := 0; i < NumDataRings; i++ {
// 		log.Printf("Ring %d: Active=%d, Inactive=%d",
// 			i, cm.ringStats[i].ActiveChunks, cm.ringStats[i].InactiveChunks)
// 	}

// 	// 检查所有数据环都有非活跃chunk
// 	for i := 0; i < NumDataRings; i++ {
// 		if cm.ringStats[i].InactiveChunks < 1 {
// 			log.Printf("EC condition not met: ring %d has no inactive chunks", i)
// 			return
// 		}
// 	}

// 	log.Printf("All rings have inactive chunks, triggering EC...")
// 	cm.performErasureCoding()
// }

// func (cm *ChunkManager) performErasureCoding() {
// 	dataChunks := cm.collectInactiveDataChunks()
// 	if len(dataChunks) != NumDataRings {
// 		log.Printf("Not enough inactive chunks: expected %d, got %d", NumDataRings, len(dataChunks))
// 		return
// 	}

// 	shards, err := cm.readAndPrepareShards(dataChunks)
// 	if err != nil {
// 		log.Printf("Error preparing shards: %v", err)
// 		return
// 	}

// 	if err := cm.encoder.Encode(shards); err != nil {
// 		log.Printf("Error encoding: %v", err)
// 		return
// 	}

// 	parityChunkIDs := cm.createParityChunks(shards[NumDataRings:])
// 	cm.createStripeMetadata(getChunkIDs(dataChunks), parityChunkIDs)
// 	cm.updateChunkStats(dataChunks)

// 	log.Printf("EC completed: created parity chunks %v", parityChunkIDs)
// }

// func (cm *ChunkManager) collectInactiveDataChunks() []*ChunkMeta {
// 	var chunks []*ChunkMeta
// 	for ringIndex := 0; ringIndex < NumDataRings; ringIndex++ {
// 		for _, chunk := range cm.chunkIndex {
// 			if uint8(ringIndex) == chunk.RingIndex && !chunk.IsActive {
// 				chunks = append(chunks, chunk)
// 				log.Printf("Selected chunk %d from ring %d for EC", chunk.ChunkID, ringIndex)
// 				break
// 			}
// 		}
// 	}
// 	return chunks
// }

// func (cm *ChunkManager) readAndPrepareShards(dataChunks []*ChunkMeta) ([][]byte, error) {
// 	shards := make([][]byte, NumDataRings+NumParityRings)
// 	maxSize := cm.getMaxChunkSize(dataChunks)

// 	// 读取数据分片
// 	for i, chunk := range dataChunks {
// 		content, err := os.ReadFile(chunk.FilePath)
// 		if err != nil {
// 			return nil, fmt.Errorf("read chunk %d: %v", chunk.ChunkID, err)
// 		}

// 		shard := make([]byte, maxSize)
// 		copy(shard, content)
// 		shards[i] = shard
// 	}

// 	// 准备校验分片内存
// 	for i := NumDataRings; i < NumDataRings+NumParityRings; i++ {
// 		shards[i] = make([]byte, maxSize)
// 	}

// 	return shards, nil
// }

// func (cm *ChunkManager) getMaxChunkSize(chunks []*ChunkMeta) int {
// 	maxSize := 0
// 	for _, chunk := range chunks {
// 		if info, err := os.Stat(chunk.FilePath); err == nil {
// 			if size := int(info.Size()); size > maxSize {
// 				maxSize = size
// 			}
// 		}
// 	}
// 	return maxSize
// }

// func (cm *ChunkManager) createParityChunks(parityShards [][]byte) []uint32 {
// 	var parityChunkIDs []uint32

// 	for i, shard := range parityShards {
// 		parityNodeIndex := TotalDataNodes + (i % (TotalParityNodes / NumParityRings)) * NumParityRings + i
// 		if parityNodeIndex >= TotalDataNodes+TotalParityNodes {
// 			parityNodeIndex = TotalDataNodes + i
// 		}

// 		nodeName := fmt.Sprintf("node%d", parityNodeIndex)
// 		chunk, err := cm.createChunk(nodeName, false, "parity")
// 		if err != nil {
// 			log.Printf("Error creating parity chunk: %v", err)
// 			continue
// 		}

// 		if err := os.WriteFile(chunk.FilePath, shard, 0644); err != nil {
// 			log.Printf("Error writing parity chunk: %v", err)
// 			continue
// 		}

// 		chunk.Size = uint32(len(shard))
// 		parityChunkIDs = append(parityChunkIDs, chunk.ChunkID)
// 	}

// 	return parityChunkIDs
// }

// func (cm *ChunkManager) createStripeMetadata(dataChunkIDs, parityChunkIDs []uint32) {
// 	stripeID := cm.nextStripeID
// 	cm.nextStripeID++

// 	stripe := &StripeMeta{
// 		StripeID:       stripeID,
// 		DataChunkIDs:   dataChunkIDs,
// 		ParityChunkIDs: parityChunkIDs,
// 		RingIndices:    make([]uint8, NumDataRings+NumParityRings),
// 	}

// 	for i := 0; i < NumDataRings+NumParityRings; i++ {
// 		if i < NumDataRings {
// 			stripe.RingIndices[i] = uint8(i)
// 		} else {
// 			stripe.RingIndices[i] = uint8(NumDataRings + (i - NumDataRings))
// 		}
// 	}

// 	cm.stripeIndex[stripeID] = stripe
// }

func (cm *ChunkManager) updateChunkStats(dataChunks []*ChunkMeta) {
	for _, chunk := range dataChunks {
		cm.ringStats[chunk.RingIndex].InactiveChunks--
	}
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
	cm.mu.Lock()
	defer cm.mu.Unlock()

	chunk, err := cm.getOrCreateActiveChunk(nodeName)
	if err != nil {
		return err
	}

	// 检查空间
	if chunk.Size+uint32(len(value)) > ChunkSize {
		chunk.IsActive = false
		cm.updateRingStats(chunk, false)
		//cm.checkAndTriggerEC()

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
	cm.mu.RLock()
	meta, exists := cm.objectIndex[key]
	if !exists {
		cm.mu.RUnlock()
		return nil, fmt.Errorf("object not found: %s", key)
	}

	chunk, exists := cm.chunkIndex[meta.ChunkID]
	if !exists {
		cm.mu.RUnlock()
		return nil, fmt.Errorf("chunk %d not found", meta.ChunkID)
	}
	cm.mu.RUnlock()

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

// 其他方法保持简洁
func (cm *ChunkManager) GetRingStats() []*RingStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := make([]*RingStats, len(cm.ringStats))
	for i, s := range cm.ringStats {
		stats[i] = &RingStats{ActiveChunks: s.ActiveChunks, InactiveChunks: s.InactiveChunks, TotalSize: s.TotalSize}
	}
	return stats
}

func (cm *ChunkManager) GetStripeInfo(stripeID uint32) (*StripeMeta, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stripe, exists := cm.stripeIndex[stripeID]
	if !exists {
		return nil, fmt.Errorf("stripe %d not found", stripeID)
	}
	return stripe, nil
}

func (cm *ChunkManager) GetStats() (objects, chunks, stripes int) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.objectIndex), len(cm.chunkIndex), len(cm.stripeIndex)
}
