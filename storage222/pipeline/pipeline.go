package pipeline

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// 流水线配置
const (
	CacheSize     = 256 * 1024 // 256KB cache
	BatchInterval = 100 * time.Millisecond
	MaxBatchSize  = 100 * 1024 // 最大批量大小 100KB
	QueueSize     = 10000
	MaxProofSize  = 10 * 1024 // 最大证明大小 10KB
)

// 流水线状态
type PipelineStatus int32

const (
	StatusStopped PipelineStatus = iota
	StatusRunning
	StatusStopping
)

// PipelineStatus 字符串表示
func (s PipelineStatus) String() string {
	switch s {
	case StatusStopped:
		return "Stopped"
	case StatusRunning:
		return "Running"
	case StatusStopping:
		return "Stopping"
	default:
		return "Unknown"
	}
}

// 下载请求
type DownloadRequest struct {
	FileName  string
	FileHash  uint64
	Data      []byte
	Conn      net.Conn // 每个请求有自己的连接
	Timestamp time.Time
	RespChan  chan *DownloadResponse
	NeedProof bool   // 是否需要证明
	NodeName  string // 节点名
}

// 下载响应
type DownloadResponse struct {
	Request    *DownloadRequest // 包含连接信息
	Data       []byte
	Error      error
	Timestamp  time.Time
	HasProof   bool     // 是否有证明
	MerklePath [][]byte // 默克尔路径
	Indices    []int64  // 兄弟节点位置
	Size       int      // 响应总大小（字节）
}

// 批量数据包
type BatchPacket struct {
	Responses []*DownloadResponse
	Timestamp time.Time
	TotalSize int // 总数据大小
}

// 批量缓存管理器
type BatchCacheManager struct {
	mu           sync.RWMutex
	batchCache   map[string]*DownloadResponse
	batchSize    int32 // 当前缓存总大小（字节）
	maxBatchSize int   // 最大缓存大小（字节）
	lastFlush    time.Time
	fileCount    int32 // 文件数量统计
}

// 流水线结构体
type Pipeline struct {
	ReceiveQueue chan *DownloadRequest
	ReadQueue    chan *DownloadRequest
	BatchCache   *BatchCacheManager
	ClientConn   net.Conn
	mu           sync.RWMutex
	status       PipelineStatus
	wg           sync.WaitGroup
	readHandler  func(string) ([]byte, error)
	stopChan     chan struct{}
	proofHandler func(string, string, []byte) ([][]byte, []int64, error) // 证明处理器
}

// ==================== 批量缓存管理器方法 ====================

// 新建批量缓存管理器
func NewBatchCacheManager(maxBatchSize int) *BatchCacheManager {
	return &BatchCacheManager{
		batchCache:   make(map[string]*DownloadResponse),
		maxBatchSize: maxBatchSize,
		lastFlush:    time.Now(),
	}
}

// 计算响应的大小
func calculateResponseSize(response *DownloadResponse) int {
	size := 0

	// 基础字段大小：哈希(8) + 是否找到(1) + 文件长度(4) + 是否有证明(1)
	size += 8 + 1 + 4 + 1

	// 文件数据大小
	size += len(response.Data)

	// 如果有证明，计算证明部分大小
	if response.HasProof {
		// 路径长度(2) + 路径数据(每个32字节) + 索引数据(每个1字节)
		size += 2 + len(response.MerklePath)*32 + len(response.Indices)
	}

	return size
}

// 添加响应到批量缓存
func (bm *BatchCacheManager) AddResponse(fileName string, response *DownloadResponse) bool {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// 计算这个响应的大小
	responseSize := calculateResponseSize(response)
	response.Size = responseSize

	// 检查是否已经存在同名文件，如果存在则替换并更新大小
	if existing, exists := bm.batchCache[fileName]; exists {
		existingSize := calculateResponseSize(existing)
		atomic.AddInt32(&bm.batchSize, -int32(existingSize))
		atomic.AddInt32(&bm.fileCount, -1)
	}

	bm.batchCache[fileName] = response
	atomic.AddInt32(&bm.batchSize, int32(responseSize))
	atomic.AddInt32(&bm.fileCount, 1)

	// 根据缓存大小决定是否刷新
	return int(atomic.LoadInt32(&bm.batchSize)) >= bm.maxBatchSize
}

// 获取批量缓存中的所有响应并清空
func (bm *BatchCacheManager) Flush() *BatchPacket {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if len(bm.batchCache) == 0 {
		return nil
	}

	responses := make([]*DownloadResponse, 0, len(bm.batchCache))
	totalSize := 0

	for _, response := range bm.batchCache {
		responses = append(responses, response)
		totalSize += response.Size
	}

	batchPacket := &BatchPacket{
		Responses: responses,
		Timestamp: time.Now(),
		TotalSize: totalSize,
	}

	// 清空批量缓存
	bm.batchCache = make(map[string]*DownloadResponse)
	atomic.StoreInt32(&bm.batchSize, 0)
	atomic.StoreInt32(&bm.fileCount, 0)
	bm.lastFlush = time.Now()

	return batchPacket
}

// 检查是否需要刷新（基于时间）
func (bm *BatchCacheManager) ShouldFlush() bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	return time.Since(bm.lastFlush) >= BatchInterval && atomic.LoadInt32(&bm.batchSize) > 0
}

// 获取批量缓存大小（字节）
func (bm *BatchCacheManager) Size() int {
	return int(atomic.LoadInt32(&bm.batchSize))
}

// 获取文件数量
func (bm *BatchCacheManager) FileCount() int {
	return int(atomic.LoadInt32(&bm.fileCount))
}

// ==================== 流水线核心方法 ====================

// 新建流水线
func NewPipeline(readHandler func(string) ([]byte, error)) *Pipeline {
	return &Pipeline{
		ReceiveQueue: make(chan *DownloadRequest, QueueSize),
		ReadQueue:    make(chan *DownloadRequest, QueueSize),
		BatchCache:   NewBatchCacheManager(MaxBatchSize),
		status:       StatusStopped,
		readHandler:  readHandler,
		stopChan:     make(chan struct{}),
	}
}

// 设置证明处理器
func (p *Pipeline) SetProofHandler(handler func(string, string, []byte) ([][]byte, []int64, error)) {
	p.proofHandler = handler
}

// 启动流水线
func (p *Pipeline) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32((*int32)(&p.status)) == int32(StatusRunning) {
		return
	}

	atomic.StoreInt32((*int32)(&p.status), int32(StatusRunning))
	p.stopChan = make(chan struct{})

	// 启动各个处理阶段
	stages := []func(){
		p.receiveStage,
		p.readStage,
		p.batchFlusher,
	}

	for _, stage := range stages {
		p.wg.Add(1)
		go stage()
	}

	log.Printf("Download pipeline started with batch interval: %v, max batch size: %d bytes",
		BatchInterval, MaxBatchSize)
}

// 停止流水线 - 优雅关闭
func (p *Pipeline) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32((*int32)(&p.status)) != int32(StatusRunning) {
		return
	}

	atomic.StoreInt32((*int32)(&p.status), int32(StatusStopping))
	close(p.stopChan)

	// 等待所有goroutine完成
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	// 带超时的等待
	select {
	case <-done:
		log.Println("Download pipeline stopped gracefully")
	case <-time.After(5 * time.Second):
		log.Println("Download pipeline stop timeout, forcing shutdown")
	}

	atomic.StoreInt32((*int32)(&p.status), int32(StatusStopped))
}

// 检查流水线是否运行
func (p *Pipeline) IsRunning() bool {
	return atomic.LoadInt32((*int32)(&p.status)) == int32(StatusRunning)
}

// 提交下载请求
func (p *Pipeline) SubmitDownloadRequest(request *DownloadRequest) bool {
	p.ClientConn = request.Conn // 设置连接
	if !p.IsRunning() {
		log.Printf("Pipeline not running, request rejected: %s", request.FileName)
		return false
	}

	request.Timestamp = time.Now()

	select {
	case p.ReceiveQueue <- request:
		log.Printf("Download request submitted: %s (needProof: %v)", request.FileName, request.NeedProof)
		return true
	case <-p.stopChan:
		return false
	default:
		log.Printf("Receive queue full, request rejected: %s", request.FileName)
		return false
	}
}

// ==================== 流水线处理阶段 ====================

// 接收阶段
func (p *Pipeline) receiveStage() {
	defer p.wg.Done()
	defer log.Println("Receive stage stopped")

	log.Println("Receive stage started")

	for {
		select {
		case request, ok := <-p.ReceiveQueue:
			if !ok {
				return
			}

			if !p.IsRunning() {
				// 如果流水线已停止，拒绝处理新请求
				log.Printf("Pipeline not running, rejecting request: %s", request.FileName)
				continue
			}

			log.Printf("Receive stage processing: %s", request.FileName)

			select {
			case p.ReadQueue <- request:
				log.Printf("Request forwarded to read stage: %s", request.FileName)
			case <-p.stopChan:
				return
			default:
				log.Printf("Read queue full, request delayed: %s", request.FileName)
				// 队列满时，等待一段时间再重试
				select {
				case p.ReadQueue <- request:
				case <-time.After(100 * time.Millisecond):
					log.Printf("Failed to forward request after retry: %s", request.FileName)
				case <-p.stopChan:
					return
				}
			}

		case <-p.stopChan:
			return
		}
	}
}

// 读取阶段
func (p *Pipeline) readStage() {
	defer p.wg.Done()
	defer log.Println("Read stage stopped")

	log.Println("Read stage started")

	for {
		select {
		case request, ok := <-p.ReadQueue:
			if !ok {
				return
			}

			if !p.IsRunning() {
				continue
			}

			log.Printf("Read stage processing: %s (needProof: %v)", request.FileName, request.NeedProof)

			var responseData []byte
			var err error

			// 读取数据
			if p.readHandler != nil {
				responseData, err = p.readHandler(request.FileName)
				if err != nil {
					log.Printf("Failed to read file %s: %v", request.FileName, err)
				}
			} else {
				err = p.simulateReadOperation(request)
				if err == nil {
					responseData = []byte(fmt.Sprintf("data_for_%s", request.FileName))
				}
			}

			response := &DownloadResponse{
				Request:   request,
				Data:      responseData,
				Error:     err,
				Timestamp: time.Now(),
			}

			// 如果需要证明，生成证明
			if request.NeedProof && p.proofHandler != nil && err == nil {
				merklePath, indices, proofErr := p.proofHandler(request.NodeName, request.FileName, responseData)
				if proofErr != nil {
					log.Printf("Failed to generate proof for %s: %v", request.FileName, proofErr)
				} else {
					response.HasProof = true
					response.MerklePath = merklePath
					response.Indices = indices
					log.Printf("Generated proof for %s: %d path elements", request.FileName, len(merklePath))
				}
			}

			// 添加到批量缓存，并根据缓存大小决定是否刷新
			if shouldFlush := p.BatchCache.AddResponse(request.FileName, response); shouldFlush {
				log.Printf("Batch size limit reached (%d bytes), flushing batch", p.BatchCache.Size())
				if batch := p.BatchCache.Flush(); batch != nil {
					p.sendBatchData(batch)
				}
			}

			log.Printf("Response cached: %s, batch size: %d bytes, files: %d, hasProof: %v",
				request.FileName, p.BatchCache.Size(), p.BatchCache.FileCount(), response.HasProof)

		case <-p.stopChan:
			return
		}
	}
}

// 批量刷新器
func (p *Pipeline) batchFlusher() {
	defer p.wg.Done()
	defer log.Println("Batch flusher stopped")

	log.Println("Batch flusher started")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !p.IsRunning() {
				return
			}

			if p.BatchCache.ShouldFlush() {
				currentSize := p.BatchCache.Size()
				log.Printf("Batch interval reached, flushing batch (size: %d bytes)", currentSize)
				if batch := p.BatchCache.Flush(); batch != nil {
					p.sendBatchData(batch)
				}
			}

		case <-p.stopChan:
			return
		}
	}
}

// ==================== 辅助方法 ====================

// 发送批量数据到客户端 - 按照新协议格式
// func (p *Pipeline) sendBatchData(batch *BatchPacket) {
// 	if p.ClientConn == nil {
// 		log.Printf("No client connection available, cannot send batch")
// 		return
// 	}

// 	// 筛选成功的响应
// 	successResponses := make([]*DownloadResponse, 0, len(batch.Responses))
// 	for _, response := range batch.Responses {
// 		if response.Error != nil {
// 			log.Printf("Skipping failed file %s: %v", response.Request.FileName, response.Error)
// 			continue
// 		}
// 		successResponses = append(successResponses, response)
// 	}

// 	if len(successResponses) == 0 {
// 		log.Printf("No successful files to send in this batch")
// 		return
// 	}

// 	log.Printf("Preparing to send %d files, total data size: %d bytes",
// 		len(successResponses), batch.TotalSize)

// 	// 创建缓冲区
// 	buffer := make([]byte, batch.TotalSize)
// 	offset := 0

// 	// 写入每个文件的数据和证明
// 	for _, response := range successResponses {
// 		// 写入文件哈希 (8字节)
// 		binary.BigEndian.PutUint64(buffer[offset:offset+8], response.Request.FileHash)
// 		offset += 8

// 		// 写入是否找到数据 (1字节) - 总是1，因为我们已经过滤了失败的
// 		buffer[offset] = 1
// 		offset += 1

// 		// 写入文件长度 (4字节)
// 		binary.BigEndian.PutUint32(buffer[offset:offset+4], uint32(len(response.Data)))
// 		offset += 4

// 		// 写入文件内容
// 		copy(buffer[offset:offset+len(response.Data)], response.Data)
// 		offset += len(response.Data)

// 		// 写入是否有证明 (1字节)
// 		if response.HasProof {
// 			buffer[offset] = 1
// 			offset += 1

// 			// 写入Merkle Path长度 (2字节)
// 			pathLength := uint16(len(response.MerklePath))
// 			binary.BigEndian.PutUint16(buffer[offset:offset+2], pathLength)
// 			offset += 2

// 			// 写入Merkle Path数据 (每个哈希32字节)
// 			for _, pathHash := range response.MerklePath {
// 				copy(buffer[offset:offset+32], pathHash)
// 				offset += 32
// 			}

// 			// 写入Indices数组 (每个1字节)
// 			for _, index := range response.Indices {
// 				buffer[offset] = byte(index)
// 				offset += 1
// 			}
// 		} else {
// 			buffer[offset] = 0
// 			offset += 1
// 		}

// 		log.Printf("Added file to batch: %s, size: %d bytes, hash: %d, hasProof: %v",
// 			response.Request.FileName, len(response.Data), response.Request.FileHash, response.HasProof)
// 	}

// 	// 验证缓冲区写入正确
// 	if offset != batch.TotalSize {
// 		log.Printf("Warning: buffer size mismatch, expected %d, got %d", batch.TotalSize, offset)
// 	}

// 	// 一次性发送所有数据
// 	startTime := time.Now()

// 	if err := writeAll(p.ClientConn, buffer); err != nil {
// 		log.Printf("Failed to send batch data: %v", err)
// 		return
// 	}

// 	duration := time.Since(startTime)
// 	log.Printf("Batch sent successfully: %d files, %d bytes, took %v",
// 		len(successResponses), batch.TotalSize, duration)
// }

func (p *Pipeline) sendBatchData(batch *BatchPacket) {
	if p.ClientConn == nil {
		log.Printf("No client connection available, cannot send batch")
		return
	}

	// 筛选成功的响应
	successResponses := make([]*DownloadResponse, 0, len(batch.Responses))
	for _, response := range batch.Responses {
		if response.Error != nil {
			log.Printf("Skipping failed file %s: %v", response.Request.FileName, response.Error)
			continue
		}
		successResponses = append(successResponses, response)
	}

	if len(successResponses) == 0 {
		log.Printf("No successful files to send in this batch")
		return
	}

	log.Printf("Preparing to send %d files with individual message headers", len(successResponses))

	// 不再需要预先分配整个缓冲区，改为逐个发送
	totalSentBytes := 0
	totalFilesSent := 0
	startTime := time.Now()

	// 逐个发送每个文件响应（每个文件都有独立的消息头）
	for _, response := range successResponses {
		// 计算这个文件响应消息体的总长度
		messageBodySize := calculateResponseSize(response)

		// 创建这个文件响应的完整消息（包含4字节消息头 + 消息体）
		message := make([]byte, 4+messageBodySize) // 4字节头 + 消息体
		offset := 0

		// 写入消息头 (4字节)
		// 第一个字节：固定标识
		message[offset] = 0xA5
		offset += 1

		// 后三个字节：消息体长度
		messageBodyLength := uint32(messageBodySize)
		message[offset] = byte((messageBodyLength >> 16) & 0xFF)
		message[offset+1] = byte((messageBodyLength >> 8) & 0xFF)
		message[offset+2] = byte(messageBodyLength & 0xFF)
		offset += 3

		// 现在 offset=4，开始写入消息体

		// 写入文件哈希 (8字节)
		binary.BigEndian.PutUint64(message[offset:offset+8], response.Request.FileHash)
		offset += 8

		// 写入是否找到数据 (1字节) - 总是1，因为我们已经过滤了失败的
		message[offset] = 1
		offset += 1

		// 写入文件长度 (4字节)
		binary.BigEndian.PutUint32(message[offset:offset+4], uint32(len(response.Data)))
		offset += 4

		// 写入文件内容
		copy(message[offset:offset+len(response.Data)], response.Data)
		offset += len(response.Data)

		// 写入是否有证明 (1字节)
		if response.HasProof {
			message[offset] = 1
			offset += 1

			// 写入Merkle Path长度 (2字节)
			pathLength := uint16(len(response.MerklePath))
			binary.BigEndian.PutUint16(message[offset:offset+2], pathLength)
			offset += 2

			// 写入Merkle Path数据 (每个哈希32字节)
			for _, pathHash := range response.MerklePath {
				copy(message[offset:offset+32], pathHash)
				offset += 32
			}

			// 写入Indices数组 (每个1字节)
			for _, index := range response.Indices {
				message[offset] = byte(index)
				offset += 1
			}
		} else {
			message[offset] = 0
			offset += 1
		}

		// 验证消息写入正确
		if offset != len(message) {
			log.Printf("Warning: message size mismatch for %s, expected %d, got %d",
				response.Request.FileName, len(message), offset)
		}

		// 发送这个文件的完整消息（包含消息头）
		if err := writeAll(p.ClientConn, message); err != nil {
			log.Printf("Failed to send file %s: %v", response.Request.FileName, err)
			continue // 继续发送其他文件，不中断
		}

		totalSentBytes += len(message)
		totalFilesSent++

		log.Printf("Sent file in batch: %s, message size: %d bytes (header: 4 bytes, body: %d bytes), hash: %d, hasProof: %v",
			response.Request.FileName, len(message), messageBodySize, response.Request.FileHash, response.HasProof)
	}

	duration := time.Since(startTime)
	log.Printf("Batch send completed: %d/%d files sent successfully, total bytes: %d, took %v",
		totalFilesSent, len(successResponses), totalSentBytes, duration)
}

// 确保 writeAll 函数存在
func writeAll(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := conn.Write(data[totalWritten:])
		if err != nil {
			return fmt.Errorf("写入失败: %w", err)
		}
		totalWritten += n
	}
	return nil
}

// 模拟读取操作
func (p *Pipeline) simulateReadOperation(request *DownloadRequest) error {
	time.Sleep(10 * time.Millisecond)
	log.Printf("Simulated read operation for: %s", request.FileName)
	return nil
}

// ==================== 状态和工具方法 ====================

// 获取流水线状态
func (p *Pipeline) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"status":           p.status.String(),
		"is_running":       p.IsRunning(),
		"receive_queue":    len(p.ReceiveQueue),
		"read_queue":       len(p.ReadQueue),
		"batch_size_bytes": p.BatchCache.Size(),
		"batch_file_count": p.BatchCache.FileCount(),
		"batch_max_size":   p.BatchCache.maxBatchSize,
	}
}

// 强制刷新批量缓存
func (p *Pipeline) FlushBatch() {
	if batch := p.BatchCache.Flush(); batch != nil {
		p.sendBatchData(batch)
		log.Printf("Manually flushed batch with %d responses, total %d bytes",
			len(batch.Responses), batch.TotalSize)
	}
}

// 设置最大批量大小
func (p *Pipeline) SetMaxBatchSize(size int) {
	p.BatchCache.maxBatchSize = size
	log.Printf("Max batch size set to %d bytes", size)
}
