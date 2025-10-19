// pipeline/pipeline.go
package pipeline

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CacheSize     = 60 * 1024 // 1MB cache (如果需要的话)
	BatchInterval = 10 * time.Second
	MaxBatchSize  = 100
	QueueSize     = 10000
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
}

// 下载响应
type DownloadResponse struct {
	Request   *DownloadRequest // 包含连接信息
	Data      []byte
	Error     error
	Timestamp time.Time
}

// 批量数据包
type BatchPacket struct {
	Responses []*DownloadResponse
	Timestamp time.Time
}

// 批量缓存管理器
type BatchCacheManager struct {
	mu           sync.RWMutex
	batchCache   map[string]*DownloadResponse
	batchSize    int32 // 使用原子操作
	maxBatchSize int
	lastFlush    time.Time
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

// 添加响应到批量缓存
func (bm *BatchCacheManager) AddResponse(fileName string, response *DownloadResponse) bool {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.batchCache[fileName] = response
	atomic.AddInt32(&bm.batchSize, 1)

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
	for _, response := range bm.batchCache {
		responses = append(responses, response)
	}

	batchPacket := &BatchPacket{
		Responses: responses,
		Timestamp: time.Now(),
	}

	// 清空批量缓存
	bm.batchCache = make(map[string]*DownloadResponse)
	atomic.StoreInt32(&bm.batchSize, 0)
	bm.lastFlush = time.Now()

	return batchPacket
}

// 检查是否需要刷新（基于时间）
func (bm *BatchCacheManager) ShouldFlush() bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	return time.Since(bm.lastFlush) >= BatchInterval && atomic.LoadInt32(&bm.batchSize) > 0
}

// 获取批量缓存大小
func (bm *BatchCacheManager) Size() int {
	return int(atomic.LoadInt32(&bm.batchSize))
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

	log.Printf("Download pipeline started with batch interval: %v, max batch size: %d",
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
	// 创建日志文件
	logFile, err := os.OpenFile("filehash.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("创建日志文件失败: %v", err)
	}
	defer logFile.Close()

	// 创建带格式的logger
	logger := log.New(logFile, "", log.LstdFlags)

	logger.Println("file: %s, hash: %d", request.FileName, request.FileHash)
	request.Timestamp = time.Now()

	select {
	case p.ReceiveQueue <- request:
		log.Printf("Download request submitted: %s", request.FileName)
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

			log.Printf("Read stage processing: %s", request.FileName)

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

			// 添加到批量缓存
			if shouldFlush := p.BatchCache.AddResponse(request.FileName, response); shouldFlush {
				log.Printf("Batch size limit reached, flushing batch")
				if batch := p.BatchCache.Flush(); batch != nil {
					p.sendBatchData(batch) // 直接发送，不通过管道
				}
			}

			log.Printf("Response cached: %s, batch size: %d",
				request.FileName, p.BatchCache.Size())

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
				log.Printf("Batch interval reached, flushing batch")
				if batch := p.BatchCache.Flush(); batch != nil {
					p.sendBatchData(batch) // 直接发送，不通过管道
				}
			}

		case <-p.stopChan:
			return
		}
	}
}

// ==================== 辅助方法 ====================

// 发送批量数据到客户端 - 将所有文件数据拼装在一起，一次发送
func (p *Pipeline) sendBatchData(batch *BatchPacket) {
	if p.ClientConn == nil {
		log.Printf("No client connection available, cannot send batch")
		return
	}

	// 计算总数据大小
	totalSize := 0
	successResponses := make([]*DownloadResponse, 0, len(batch.Responses))

	// 筛选成功的响应并计算总大小
	for _, response := range batch.Responses {
		if response.Error != nil {
			log.Printf("Skipping failed file %s: %v", response.Request.FileName, response.Error)
			continue
		}
		// 每个文件需要: 8字节哈希 + 8字节长度 + 文件内容
		totalSize += 8 + 8 + len(response.Data)
		successResponses = append(successResponses, response)
	}

	if len(successResponses) == 0 {
		log.Printf("No successful files to send in this batch")
		return
	}

	log.Printf("Preparing to send %d files, total data size: %d bytes",
		len(successResponses), totalSize)

	// 创建缓冲区
	buffer := make([]byte, totalSize)
	offset := 0

	// 写入每个文件的数据
	for _, response := range successResponses {
		// 写入文件哈希 (8字节)
		binary.BigEndian.PutUint64(buffer[offset:offset+8], response.Request.FileHash)
		offset += 8

		// 写入文件长度 (8字节)
		binary.BigEndian.PutUint64(buffer[offset:offset+8], uint64(len(response.Data)))
		offset += 8

		// 写入文件内容
		copy(buffer[offset:offset+len(response.Data)], response.Data)
		offset += len(response.Data)

		log.Printf("Added file to batch: %s, size: %d, hash: %d",
			response.Request.FileName, len(response.Data), response.Request.FileHash)
	}

	// 一次性发送所有数据
	startTime := time.Now()
	//log.Printf("%s", buffer)
	log.Printf("  客户端: %s", p.ClientConn.RemoteAddr().String())
	log.Printf("  服务器: %s", p.ClientConn.LocalAddr().String())
	// n, err := p.ClientConn.Write(buffer)
	if err := writeAll(p.ClientConn, buffer); err != nil {
		log.Printf("写入数据失败: %v", err)
		return
	}
	duration := time.Since(startTime)
	log.Printf("took %v", duration)
	/*
		 	if err != nil {
				log.Printf("Failed to send batch data: %v", err)
				return
			}

			log.Printf("Batch sent successfully: %d files, %d bytes, took %v",
				len(successResponses), n, duration)
	*/
}

func writeAll(conn net.Conn, data []byte) error {
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
		"status":         p.status.String(),
		"is_running":     p.IsRunning(),
		"receive_queue":  len(p.ReceiveQueue),
		"read_queue":     len(p.ReadQueue),
		"batch_size":     p.BatchCache.Size(),
		"batch_max_size": p.BatchCache.maxBatchSize,
	}
}

// 强制刷新批量缓存
func (p *Pipeline) FlushBatch() {
	if batch := p.BatchCache.Flush(); batch != nil {
		p.sendBatchData(batch)
		log.Printf("Manually flushed batch with %d responses", len(batch.Responses))
	}
}
