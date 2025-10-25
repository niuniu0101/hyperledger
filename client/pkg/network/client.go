package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ...existing code...

// TCPClient 维护与单个服务器的长连接
type TCPClient struct {
	serverAddr string
	timeout    time.Duration
	helper     *ProtocolHelper
	Conn       net.Conn
}

// NewTCPClient 创建客户端（尚未连接）
func NewTCPClient(serverAddr string) *TCPClient {
	return &TCPClient{
		serverAddr: serverAddr,
		timeout:    10 * time.Second,
		helper:     &ProtocolHelper{},
	}
}

// ServerAddr 返回服务器地址（用于观测日志）
func (c *TCPClient) ServerAddr() string {
	return c.serverAddr
}

// SetTimeout 设置读写超时
func (c *TCPClient) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

// Connect 主动建立长连接
func (c *TCPClient) Connect() error {
	if c.Conn != nil {
		return nil
	}
	dialer := &net.Dialer{
		Timeout:   c.timeout,
		KeepAlive: 100 * time.Hour,
	}
	conn, err := dialer.Dial("tcp", c.serverAddr)
	if err != nil {
		return err
	}
	c.Conn = conn
	return nil
}

// Close 关闭长连接
func (c *TCPClient) Close() error {
	return c.resetConn()
}

func (c *TCPClient) resetConn() error {
	if c.Conn != nil {
		_ = c.Conn.Close()
		c.Conn = nil
	}
	return nil
}

func (c *TCPClient) ensureConn() (net.Conn, error) {
	if c.Conn != nil {
		return c.Conn, nil
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c.Conn, nil
}

func (c *TCPClient) setWriteDeadline(conn net.Conn) error {
	if c.timeout <= 0 {
		return nil
	}
	//return conn.SetWriteDeadline(time.Now().Add(c.timeout))
	return conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
}

func (c *TCPClient) setReadDeadline(conn net.Conn) error {
	if c.timeout <= 0 {
		return nil
	}
	//return conn.SetReadDeadline(time.Now().Add(c.timeout))
	return conn.SetReadDeadline(time.Now().Add(120 * time.Second))
}

func (c *TCPClient) clearDeadlines(conn net.Conn) {
	if c.timeout <= 0 {
		return
	}
	_ = conn.SetDeadline(time.Time{})
}

// UploadFile 使用持久连接上传
func (c *TCPClient) UploadFile(nodeName, fileName string, fileHash uint64, fileData []byte) error {
	conn, err := c.ensureConn()
	if err != nil {
		return err
	}
	helper := c.helper

	if err := c.setWriteDeadline(conn); err != nil {
		// c.resetConn()
		return err
	}
	if _, err := conn.Write([]byte{1}); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteUint64(conn, fileHash); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteString(conn, fileName, 16); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteString(conn, nodeName, 8); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteUint64(conn, uint64(len(fileData))); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.SendFileData(conn, fileData); err != nil {
		// c.resetConn()
		return err
	}
	c.clearDeadlines(conn)
	return nil
}

// QueryFile 使用持久连接查询
// 返回服务器返回的文件哈希 (uint64), 文件数据, error
func (c *TCPClient) QueryFile(nodeName string, fileHash uint64) (uint64, []byte, error) {
	conn, err := c.ensureConn()
	if err != nil {
		return 0, nil, err
	}
	helper := c.helper

	if err := c.setWriteDeadline(conn); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	if _, err := conn.Write([]byte{0}); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	if err := helper.WriteString(conn, nodeName, 8); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	if err := helper.WriteUint64(conn, fileHash); err != nil {
		// c.resetConn()
		return 0, nil, err
	}

	if err := c.setReadDeadline(conn); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	hashBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, hashBuf); err != nil {

		log.Printf("%d", hashBuf)
		// c.resetConn()
		return 0, nil, fmt.Errorf("failed to read file hash from server: %w", err)
	}
	if string(hashBuf) == "00000000" {
		// fmt.Println("failed to read file hash from server")
		return 0, nil, fmt.Errorf("failed to read file hash from server, reveive 00000000")
	}
	returnedHash := binary.BigEndian.Uint64(hashBuf)

	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		log.Printf("%d", lenBuf)
		// c.resetConn()
		return 0, nil, fmt.Errorf("failed to read file length from server: %w", err)
	}

	fileLen := binary.BigEndian.Uint64(lenBuf)
	const MaxFileSize = 100 * 1024 * 1024 // 100MB
	if fileLen == 0 {
		// 存储层语义：未命中时返回长度0
		c.clearDeadlines(conn)
		return returnedHash, nil, fmt.Errorf("not found")
	}
	if fileLen > MaxFileSize {
		// c.resetConn()
		return 0, nil, fmt.Errorf("invalid file size: %d", fileLen)
	}
	data, err := helper.ReceiveFileData(conn, fileLen)
	if err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	c.clearDeadlines(conn)
	return returnedHash, data, nil
}

// AsyncQueryResult 异步查询返回结果
type AsyncQueryResult struct {
	ReturnedHash uint64 // 成功读取到的返回 hash（一般等于请求 hash）
	RequestHash  uint64 // 对应请求的 hash（写入失败等场景用于回退）
	NodeName     string // 对应请求的 node（用于回退中心查询）
	Data         []byte
	Err          error
}

// AsyncQuerySession 基于单条 TCP 连接的异步查询会话
// - writer 协程不断发送请求帧（不等待响应）
// - reader 协程持续读取响应帧并经 Results 通道吐出
// - 支持多worker模式：主reader读取响应后分发给多个worker处理
type AsyncQuerySession struct {
	client *TCPClient
	sendCh chan struct {
		NodeName string
		FileHash uint64
	}
	results       chan AsyncQueryResult
	closed        chan struct{}
	mu            sync.Mutex
	pending       map[uint64]pendingRequest // fileHash -> 请求信息
	inFlight      int64
	inFlightLimit int64

	// 多worker支持
	workerCount int
	responseCh  chan rawResponse // 主reader到worker的响应分发通道
	workerWG    sync.WaitGroup   // worker goroutine等待组
}

// rawResponse 原始响应数据，由主reader读取后分发给worker处理
type rawResponse struct {
	HashBuf   []byte // 8字节hash
	LenBuf    []byte // 8字节长度
	Data      []byte // 文件数据
	Err       error  // 读取错误
	Timestamp time.Time
}

// pendingRequest 记录待响应的请求信息
type pendingRequest struct {
	NodeName  string
	Timestamp time.Time
}

func (s *AsyncQuerySession) popPending() (uint64, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h, req := range s.pending {
		delete(s.pending, h)
		return h, req.NodeName, true
	}
	return 0, "", false
}

// removePending 移除指定的pending请求并返回节点名
func (s *AsyncQuerySession) removePending(fileHash uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req, exists := s.pending[fileHash]; exists {
		delete(s.pending, fileHash)
		return req.NodeName, true
	}
	return "", false
}

// cleanupTimeoutRequests 清理超时的请求
func (s *AsyncQuerySession) cleanupTimeoutRequests(timeout time.Duration) []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var timeoutHashes []uint64
	now := time.Now()

	for hash, req := range s.pending {
		if now.Sub(req.Timestamp) > timeout {
			timeoutHashes = append(timeoutHashes, hash)
		}
	}

	// 移除超时请求
	for _, hash := range timeoutHashes {
		delete(s.pending, hash)
		atomic.AddInt64(&s.inFlight, -1)
	}

	return timeoutHashes
}

// Metrics 返回 in-flight、pending 长度、发送队列长度、并发上限
func (s *AsyncQuerySession) Metrics() (int64, int, int, int64) {
	s.mu.Lock()
	p := len(s.pending)
	s.mu.Unlock()
	return atomic.LoadInt64(&s.inFlight), p, len(s.sendCh), s.inFlightLimit
}

// SetInFlightLimit 设置每条连接允许的最大并发请求数（超过将拒绝新投递）
func (s *AsyncQuerySession) SetInFlightLimit(limit int64) {

	limit = 100000

	s.inFlightLimit = limit
}

// SetWorkerCount 设置worker goroutine数量
func (s *AsyncQuerySession) SetWorkerCount(count int) {
	if count <= 0 {
		count = 1
	}
	s.workerCount = count
}

// WorkerCount 返回当前worker goroutine数量
func (s *AsyncQuerySession) WorkerCount() int {
	return s.workerCount
}

// startWorkers 启动多个worker goroutine处理响应
func (s *AsyncQuerySession) startWorkers() {
	for i := 0; i < s.workerCount; i++ {
		s.workerWG.Add(1)
		go func(workerID int) {
			defer s.workerWG.Done()
			s.workerLoop(workerID)
		}(i)
	}
}

// workerLoop worker goroutine的主循环
func (s *AsyncQuerySession) workerLoop(workerID int) {
	const MaxFileSize = 100 * 1024 * 1024

	for {
		select {
		case <-s.closed:
			return
		case resp, ok := <-s.responseCh:
			if !ok {
				return
			}

			// 处理响应
			if resp.Err != nil {
				// 读取错误，需要从pending中移除一个请求
				if h, n, ok := s.popPending(); ok {
					atomic.AddInt64(&s.inFlight, -1)
					s.results <- AsyncQueryResult{
						Err:         resp.Err,
						RequestHash: h,
						NodeName:    n,
					}
				}
				continue
			}

			returnedHash := binary.BigEndian.Uint64(resp.HashBuf)

			// 取出对应的nodeName并清理pending
			nodeName, exists := s.removePending(returnedHash)
			if !exists {
				// 如果找不到对应的pending请求，说明可能已经被清理了
				continue
			}
			atomic.AddInt64(&s.inFlight, -1)

			fileLen := binary.BigEndian.Uint64(resp.LenBuf)
			if fileLen == 0 {
				// 存储层语义：未找到时返回长度0
				s.results <- AsyncQueryResult{
					ReturnedHash: returnedHash,
					RequestHash:  returnedHash,
					NodeName:     nodeName,
					Err:          fmt.Errorf("not found"),
				}
				continue
			}

			if fileLen > MaxFileSize {
				s.results <- AsyncQueryResult{
					Err:         fmt.Errorf("invalid file size: %d", fileLen),
					RequestHash: returnedHash,
					NodeName:    nodeName,
				}
				continue
			}

			// 数据已经在主reader中读取完成，直接使用
			s.results <- AsyncQueryResult{
				ReturnedHash: returnedHash,
				RequestHash:  returnedHash,
				NodeName:     nodeName,
				Data:         resp.Data,
			}
		}
	}
}

// TryEnqueue 非阻塞投递，请求被接受返回 true，否则返回 false
func (s *AsyncQuerySession) TryEnqueue(nodeName string, fileHash uint64) bool {
	// 限流
	if s.inFlightLimit > 0 && atomic.LoadInt64(&s.inFlight) >= s.inFlightLimit {
		return false
	}
	// 先登记 pending 与 inFlight，确保错误路径可恢复上下文
	s.mu.Lock()
	s.pending[fileHash] = pendingRequest{
		NodeName:  nodeName,
		Timestamp: time.Now(),
	}
	s.mu.Unlock()
	atomic.AddInt64(&s.inFlight, 1)
	// 非阻塞送入通道
	select {
	case s.sendCh <- struct {
		NodeName string
		FileHash uint64
	}{NodeName: nodeName, FileHash: fileHash}:
		return true
	default:
		// 回滚登记
		s.mu.Lock()
		delete(s.pending, fileHash)
		s.mu.Unlock()
		atomic.AddInt64(&s.inFlight, -1)
		return false
	}
}

// StartAsyncQuery 启动异步查询会话
func (c *TCPClient) StartAsyncQuery(buffer int) (*AsyncQuerySession, error) {
	return c.StartAsyncQueryWithWorkers(buffer, 1)
}

// StartAsyncQueryWithWorkers 启动异步查询会话，支持指定worker数量
func (c *TCPClient) StartAsyncQueryWithWorkers(buffer int, workerCount int) (*AsyncQuerySession, error) {
	/* 	if buffer <= 0 {
		buffer = 256
	} */
	buffer = 10000
	if workerCount <= 0 {
		workerCount = 1
	}
	if _, err := c.ensureConn(); err != nil {
		return nil, err
	}
	s := &AsyncQuerySession{
		client: c,
		sendCh: make(chan struct {
			NodeName string
			FileHash uint64
		}, buffer),
		results:     make(chan AsyncQueryResult, buffer),
		closed:      make(chan struct{}),
		pending:     make(map[uint64]pendingRequest, buffer),
		workerCount: workerCount,
		responseCh:  make(chan rawResponse, buffer*2), // 响应通道容量是buffer的2倍
	}

	// writer 协程
	go func() {
		for {
			select {
			case <-s.closed:
				return
			case req, ok := <-s.sendCh:
				if !ok {
					return
				}
				conn, err := s.client.ensureConn()
				if err != nil {
					if nodeName, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				if err := s.client.setWriteDeadline(conn); err != nil {
					if nodeName, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				// 写入 op(0)
				if _, err := conn.Write([]byte{0}); err != nil {
					if nodeName, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				// 写入 node(8) - 确保总是发送8字节
				var nodeBuf [8]byte
				copy(nodeBuf[:], []byte(req.NodeName))
				if _, err := conn.Write(nodeBuf[:]); err != nil {
					if nodeName, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				// 写入 hash(8)
				var hbuf [8]byte
				binary.BigEndian.PutUint64(hbuf[:], req.FileHash)
				if _, err := conn.Write(hbuf[:]); err != nil {
					if nodeName, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				s.client.clearDeadlines(conn)
			}
		}
	}()

	// 启动worker goroutines
	s.startWorkers()

	// 超时清理协程
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.closed:
				return
			case <-ticker.C:
				// 清理超过30秒的请求
				timeoutHashes := s.cleanupTimeoutRequests(30 * time.Second)
				if len(timeoutHashes) > 0 {
					log.Printf("[超时清理] 清理了 %d 个超时请求", len(timeoutHashes))
					for _, hash := range timeoutHashes {
						// 确保hash不为0
						if hash == 0 {
							log.Printf("[超时清理] 跳过hash=0的请求")
							continue
						}
						if nodeName, exists := s.removePending(hash); exists {
							s.results <- AsyncQueryResult{
								Err:         fmt.Errorf("request timeout after 30s"),
								RequestHash: hash,
								NodeName:    nodeName,
							}
						}
					}
				}
			}
		}
	}()

	// 连接健康检查协程 - 仅检查连接状态，不发送心跳包
	go func() {
		healthTicker := time.NewTicker(30 * time.Second)
		defer healthTicker.Stop()
		for {
			select {
			case <-s.closed:
				return
			case <-healthTicker.C:
				// 仅检查连接是否还活着，不发送任何数据
				_, err := s.client.ensureConn()
				if err != nil {
					log.Printf("[健康检查] 连接检查失败: %v", err)
				} else {
					// 连接正常，记录日志但不发送数据
					log.Printf("[健康检查] 连接状态正常")
				}
			}
		}
	}()

	// 主reader协程 - 只负责读取原始数据并分发给worker
	go func() {
		const MaxFileSize = 100 * 1024 * 1024
		for {
			select {
			case <-s.closed:
				// close(s.responseCh)
				// close(s.results)
				return
			default:
			}
			conn, err := s.client.ensureConn()
			if err != nil {
				// 连接失败，发送错误给worker处理
				select {
				case s.responseCh <- rawResponse{Err: err, Timestamp: time.Now()}:
				case <-s.closed:
					return
				}
				continue
			}
			if err := s.client.setReadDeadline(conn); err != nil {
				select {
				case s.responseCh <- rawResponse{Err: err, Timestamp: time.Now()}:
				case <-s.closed:
					return
				}
				continue
			}

			// 读取hash
			hashBuf := make([]byte, 8)
			if _, err := io.ReadFull(conn, hashBuf); err != nil {
				select {
				case s.responseCh <- rawResponse{Err: fmt.Errorf("failed to read file hash from server: %w", err), Timestamp: time.Now()}:
				case <-s.closed:
					return
				}
				continue
			}
			receivedHash := binary.BigEndian.Uint64(hashBuf)
			fmt.Println("%d", receivedHash)
			// 读取长度
			lenBuf := make([]byte, 8)
			if _, err := io.ReadFull(conn, lenBuf); err != nil {
				select {
				case s.responseCh <- rawResponse{Err: fmt.Errorf("failed to read file length from server: %w", err), Timestamp: time.Now()}:
				case <-s.closed:
					return
				}
				continue
			}

			fileLen := binary.BigEndian.Uint64(lenBuf)
			var data []byte

			// 如果文件长度大于0，读取文件数据
			if fileLen > 0 {
				if fileLen > MaxFileSize {
					select {
					case s.responseCh <- rawResponse{Err: fmt.Errorf("invalid file size: %d", fileLen), Timestamp: time.Now()}:
					case <-s.closed:
						return
					}
					continue
				}

				data, err = s.client.helper.ReceiveFileData(conn, fileLen)
				if err != nil {
					select {
					case s.responseCh <- rawResponse{Err: err, Timestamp: time.Now()}:
					case <-s.closed:
						return
					}
					continue
				}
			}

			s.client.clearDeadlines(conn)

			// 将原始响应数据分发给worker处理
			select {
			case s.responseCh <- rawResponse{
				HashBuf:   hashBuf,
				LenBuf:    lenBuf,
				Data:      data,
				Timestamp: time.Now(),
			}:
			case <-s.closed:
				return
			}
		}
	}()

	return s, nil
}

// Enqueue 发送一个查询请求
func (s *AsyncQuerySession) Enqueue(nodeName string, fileHash uint64) {
	s.sendCh <- struct {
		NodeName string
		FileHash uint64
	}{NodeName: nodeName, FileHash: fileHash}
}

// Results 返回结果通道
func (s *AsyncQuerySession) Results() <-chan AsyncQueryResult {
	return s.results
}

// Close 关闭会话
func (s *AsyncQuerySession) Close() {
	select {
	case <-s.closed:
		return
	default:
	}
	close(s.closed)
	close(s.sendCh)

	// 等待所有worker完成
	s.workerWG.Wait()
}
