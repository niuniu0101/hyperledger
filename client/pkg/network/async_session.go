package network

import (
	"sync"
	"sync/atomic"
	"time"
)

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
	// 按字节流控
	inFlightBytes      int64
	inFlightBytesLimit int64

	// 多worker支持
	workerCount int
	responseCh  chan rawResponse // 主reader到worker的响应分发通道
	workerWG    sync.WaitGroup   // worker goroutine等待组
	// 当 needProof 为 true 时，writer 会在请求中设置 needProof 字段，
	// reader/worker 会尝试对收到的证明进行验证。如果 trustedRootLookup
	// 非 nil，则在验证时调用该函数获取可信根（hex string）。
	needProof         bool
	trustedRootLookup func(nodeName string) (string, error)
}

// --- 全局挂起请求路由表：用于跨连接转发响应 ---
// 某些服务端会把不同请求的响应回到同一条连接上，为了避免“响应落在错误的连接上”被丢弃，
// 这里维护一个 hash->session 的全局索引。任意 reader 如果收到了一个本地 pending 不存在的 hash，
// 可以通过该索引把原始响应转发到真正拥有该 hash 的会话上。
type pendingRegistry struct {
	mu  sync.RWMutex
	byH map[uint64]*AsyncQuerySession
}

var globalPending = &pendingRegistry{byH: make(map[uint64]*AsyncQuerySession)}

func (r *pendingRegistry) add(h uint64, s *AsyncQuerySession) {
	if h == 0 || s == nil {
		return
	}
	r.mu.Lock()
	r.byH[h] = s
	r.mu.Unlock()
}

func (r *pendingRegistry) get(h uint64) *AsyncQuerySession {
	r.mu.RLock()
	s := r.byH[h]
	r.mu.RUnlock()
	return s
}

func (r *pendingRegistry) remove(h uint64) {
	if h == 0 {
		return
	}
	r.mu.Lock()
	delete(r.byH, h)
	r.mu.Unlock()
}

func (r *pendingRegistry) removeAllOfSession(s *AsyncQuerySession) {
	r.mu.Lock()
	for h, owner := range r.byH {
		if owner == s {
			delete(r.byH, h)
		}
	}
	r.mu.Unlock()
}

func (s *AsyncQuerySession) popPending() (uint64, string, uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h, req := range s.pending {
		delete(s.pending, h)
		globalPending.remove(h)
		return h, req.NodeName, req.ExpectedSize, true
	}
	return 0, "", 0, false
}

// removePending 移除指定的pending请求并返回节点名
func (s *AsyncQuerySession) removePending(fileHash uint64) (string, uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req, exists := s.pending[fileHash]; exists {
		delete(s.pending, fileHash)
		globalPending.remove(fileHash)
		return req.NodeName, req.ExpectedSize, true
	}
	return "", 0, false
}

// timeoutEntry 表示一个已超时的挂起请求的快照
type timeoutEntry struct {
	Hash         uint64
	NodeName     string
	ExpectedSize uint32
}

// cleanupTimeoutRequests 清理超时的请求，返回被清理的条目（包含 node 信息）
// 说明：在此函数内完成从 pending 中的删除与 inFlight 的扣减，调用方负责处理 inFlightBytes 与结果上报。
func (s *AsyncQuerySession) cleanupTimeoutRequests(timeout time.Duration) []timeoutEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var out []timeoutEntry
	for hash, req := range s.pending {
		if now.Sub(req.Timestamp) > timeout {
			out = append(out, timeoutEntry{Hash: hash, NodeName: req.NodeName, ExpectedSize: req.ExpectedSize})
		}
	}
	// 移除并更新 inFlight
	if len(out) > 0 {
		for _, e := range out {
			delete(s.pending, e.Hash)
			globalPending.remove(e.Hash)
			atomic.AddInt64(&s.inFlight, -1)
		}
	}
	return out
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

	// 如果传入非正值，则使用默认上限
	if limit <= 0 {
		limit = 100000
	}
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
	globalPending.add(fileHash, s)
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
		globalPending.remove(fileHash)
		atomic.AddInt64(&s.inFlight, -1)
		return false
	}
}

// SetInFlightBytesLimit 设置按字节计的并发上限（字节）
func (s *AsyncQuerySession) SetInFlightBytesLimit(limit int64) {
	s.inFlightBytesLimit = limit
}

// TryEnqueueWithSize 非阻塞按预期大小入队，成功返回 true，否则返回 false
func (s *AsyncQuerySession) TryEnqueueWithSize(nodeName string, fileHash uint64, expectedSize uint32) bool {
	if s.inFlightBytesLimit > 0 && atomic.LoadInt64(&s.inFlightBytes)+int64(expectedSize) > s.inFlightBytesLimit {
		return false
	}
	s.mu.Lock()
	s.pending[fileHash] = pendingRequest{
		NodeName:     nodeName,
		Timestamp:    time.Now(),
		ExpectedSize: expectedSize,
	}
	s.mu.Unlock()
	globalPending.add(fileHash, s)
	atomic.AddInt64(&s.inFlight, 1)
	if expectedSize > 0 {
		atomic.AddInt64(&s.inFlightBytes, int64(expectedSize))
	}
	select {
	case s.sendCh <- struct {
		NodeName string
		FileHash uint64
	}{NodeName: nodeName, FileHash: fileHash}:
		return true
	default:
		// 回滚
		s.mu.Lock()
		delete(s.pending, fileHash)
		s.mu.Unlock()
		globalPending.remove(fileHash)
		atomic.AddInt64(&s.inFlight, -1)
		if expectedSize > 0 {
			atomic.AddInt64(&s.inFlightBytes, -int64(expectedSize))
		}
		return false
	}
}

// EnqueueWithSize 阻塞入队，直到成功或会话关闭
func (s *AsyncQuerySession) EnqueueWithSize(nodeName string, fileHash uint64, expectedSize uint32) {
	// 先reserve字节（阻塞直到有足够配额或会话关闭）
	for {
		if s.inFlightBytesLimit == 0 {
			break
		}
		cur := atomic.LoadInt64(&s.inFlightBytes)
		if cur+int64(expectedSize) <= s.inFlightBytesLimit {
			if atomic.CompareAndSwapInt64(&s.inFlightBytes, cur, cur+int64(expectedSize)) {
				break
			}
			continue
		}
		// 等待一会儿或会话关闭
		select {
		case <-time.After(10 * time.Millisecond):
			continue
		case <-s.closed:
			return
		}
	}

	s.mu.Lock()
	s.pending[fileHash] = pendingRequest{
		NodeName:     nodeName,
		Timestamp:    time.Now(),
		ExpectedSize: expectedSize,
	}
	s.mu.Unlock()
	globalPending.add(fileHash, s)
	atomic.AddInt64(&s.inFlight, 1)

	select {
	case s.sendCh <- struct {
		NodeName string
		FileHash uint64
	}{NodeName: nodeName, FileHash: fileHash}:
		return
	case <-s.closed:
		// rollback
		s.mu.Lock()
		delete(s.pending, fileHash)
		s.mu.Unlock()
		globalPending.remove(fileHash)
		atomic.AddInt64(&s.inFlight, -1)
		if expectedSize > 0 {
			atomic.AddInt64(&s.inFlightBytes, -int64(expectedSize))
		}
		return
	}
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

	// 清理全局索引中属于本会话的项
	globalPending.removeAllOfSession(s)
}
