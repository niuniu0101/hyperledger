package network

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

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
				if h, n, expected, ok := s.popPending(); ok {
					atomic.AddInt64(&s.inFlight, -1)
					if expected > 0 {
						atomic.AddInt64(&s.inFlightBytes, -int64(expected))
					}
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
			nodeName, expectedSize, exists := s.removePending(returnedHash)
			if !exists {
				// 本会话未找到 pending，尝试通过全局路由转发给真正的拥有者
				if owner := globalPending.get(returnedHash); owner != nil && owner != s {
					select {
					case owner.responseCh <- resp:
						// 已转发，由拥有者会话处理
						continue
					default:
						// 拥有者队列已满，为避免阻塞，此处丢弃并记录结果为错误
						s.results <- AsyncQueryResult{
							Err:         fmt.Errorf("owner session queue full; dropped cross-conn response"),
							RequestHash: returnedHash,
							NodeName:    "",
						}
						continue
					}
				}
				// 没有拥有者信息，只能丢弃
				continue
			}
			atomic.AddInt64(&s.inFlight, -1)
			if expectedSize > 0 {
				atomic.AddInt64(&s.inFlightBytes, -int64(expectedSize))
			}

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

			// 如果需要验证 proof，则在这里进行验证（worker 中执行，避免阻塞 reader）
			if s.needProof && len(resp.ProofData) > 0 && s.trustedRootLookup != nil {
				// 计算根
				computedRoot, verr := s.client.helper.VerifyMerkleProof(resp.Data, resp.ProofData, resp.IndicesData)
				if verr != nil {
					s.results <- AsyncQueryResult{
						Err:         fmt.Errorf("failed to compute merkle root: %w", verr),
						RequestHash: returnedHash,
						NodeName:    nodeName,
					}
					continue
				}
				// 获取可信根（hex string）并比较
				trustedHex, terr := s.trustedRootLookup(nodeName)
				if terr != nil {
					s.results <- AsyncQueryResult{
						Err:         fmt.Errorf("failed to fetch trusted root: %w", terr),
						RequestHash: returnedHash,
						NodeName:    nodeName,
					}
					continue
				}
				trustedBytes, derr := hex.DecodeString(trustedHex)
				if derr != nil {
					s.results <- AsyncQueryResult{
						Err:         fmt.Errorf("invalid trusted root hex: %w", derr),
						RequestHash: returnedHash,
						NodeName:    nodeName,
					}
					continue
				}
				if !bytesEqual(trustedBytes, computedRoot) {
					s.results <- AsyncQueryResult{
						Err:         fmt.Errorf("merkle proof verification failed: computed %x != trusted %x", computedRoot, trustedBytes),
						RequestHash: returnedHash,
						NodeName:    nodeName,
					}
					continue
				}
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

// StartAsyncQuery 启动异步查询会话
func (c *TCPClient) StartAsyncQuery(buffer int) (*AsyncQuerySession, error) {
	return c.StartAsyncQueryWithWorkers(buffer, 1, false, nil)
}

// StartAsyncQueryWithWorkers 启动异步查询会话，支持指定worker数量
// needProof: 如果为 true，writer 会在请求中设置 needProof 字段（请求服务器返回 merkle proof）
// trustedRootLookup: 可选函数，用于在收到 proof 后获取可信根（hex string），函数签名为 func(nodeName string) (string, error)
func (c *TCPClient) StartAsyncQueryWithWorkers(buffer int, workerCount int, needProof bool, trustedRootLookup func(string) (string, error)) (*AsyncQuerySession, error) {
	// 使用合理的默认 buffer（仅在 caller 未设置或设置为非正时）
	if buffer <= 0 {
		buffer = 10000
	}
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
		results:           make(chan AsyncQueryResult, buffer),
		closed:            make(chan struct{}),
		pending:           make(map[uint64]pendingRequest, buffer),
		workerCount:       workerCount,
		responseCh:        make(chan rawResponse, buffer*2), // 响应通道容量是buffer的2倍
		needProof:         needProof,
		trustedRootLookup: trustedRootLookup,
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
					log.Printf("[writer] addr=%s ensureConn error: %v (hash=%d node=%s)", s.client.ServerAddr(), err, req.FileHash, req.NodeName)
					if nodeName, expected, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						if expected > 0 {
							atomic.AddInt64(&s.inFlightBytes, -int64(expected))
						}
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				if err := s.client.setWriteDeadline(conn); err != nil {
					log.Printf("[writer] addr=%s setWriteDeadline error: %v (hash=%d node=%s)", s.client.ServerAddr(), err, req.FileHash, req.NodeName)
					if nodeName, expected, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						if expected > 0 {
							atomic.AddInt64(&s.inFlightBytes, -int64(expected))
						}
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				// 使用新帧协议构建并写入下载请求帧
				need := byte(0)
				if s.needProof {
					need = 1
				}
				payload := s.client.helper.BuildDownloadPayload(req.NodeName, req.FileHash, need)
				log.Printf("[writer] addr=%s sending frame hash=%d node=%s", s.client.ServerAddr(), req.FileHash, req.NodeName)
				if err := s.client.helper.WriteFrame(conn, payload); err != nil {
					log.Printf("[writer] addr=%s WriteFrame error: %v (hash=%d node=%s)", s.client.ServerAddr(), err, req.FileHash, req.NodeName)
					if nodeName, expected, exists := s.removePending(req.FileHash); exists {
						atomic.AddInt64(&s.inFlight, -1)
						if expected > 0 {
							atomic.AddInt64(&s.inFlightBytes, -int64(expected))
						}
						s.results <- AsyncQueryResult{Err: err, RequestHash: req.FileHash, NodeName: nodeName}
					}
					continue
				}
				log.Printf("[writer] addr=%s sent frame hash=%d", s.client.ServerAddr(), req.FileHash)
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
				// 清理超过读取超时时间的请求（与 ReadDeadline 保持一致，避免误删仍可能返回的请求）
				timeouts := s.cleanupTimeoutRequests(2 * time.Minute)
				if len(timeouts) > 0 {
					log.Printf("[超时清理] 清理了 %d 个超时请求 (>=2m)", len(timeouts))
					for _, e := range timeouts {
						if e.Hash == 0 {
							log.Printf("[超时清理] 跳过hash=0的请求")
							continue
						}
						// inFlight 已在 cleanup 内扣减；这里补充字节额度回收并上报结果
						if e.ExpectedSize > 0 {
							atomic.AddInt64(&s.inFlightBytes, -int64(e.ExpectedSize))
						}
						s.results <- AsyncQueryResult{
							Err:         fmt.Errorf("request timeout after 30s"),
							RequestHash: e.Hash,
							NodeName:    e.NodeName,
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

			// 使用帧协议读取 payload 并解析
			payload, err := s.client.helper.ReadFrame(conn, 200*1024*1024) // 最大接受200MB
			if err != nil {
				log.Printf("[reader] addr=%s ReadFrame error: %v", s.client.ServerAddr(), err)
				select {
				case s.responseCh <- rawResponse{Err: fmt.Errorf("failed to read frame from server: %w", err), Timestamp: time.Now()}:
				case <-s.closed:
					return
				}
				continue
			}
			// 解析存储层响应（包含可选的 merkle proof）
			returnedHash, _, data, proofData, indicesData, perr := s.client.helper.ParseStorageResponse(payload)
			if perr != nil {
				log.Printf("[reader] addr=%s ParseStorageResponse error: %v", s.client.ServerAddr(), perr)
				select {
				case s.responseCh <- rawResponse{Err: perr, Timestamp: time.Now()}:
				case <-s.closed:
					return
				}
				continue
			}

			// 构造兼容的 HashBuf/LenBuf/Data 以供 worker 使用
			var hashBuf [8]byte
			binary.BigEndian.PutUint64(hashBuf[:], returnedHash)
			var lenBuf [8]byte
			binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
			s.client.clearDeadlines(conn)
			log.Printf("[reader] addr=%s received frame hash=%d len=%d", s.client.ServerAddr(), returnedHash, len(data))
			select {
			case s.responseCh <- rawResponse{
				HashBuf:     hashBuf[:],
				LenBuf:      lenBuf[:],
				Data:        data,
				ProofData:   proofData,
				IndicesData: indicesData,
				Timestamp:   time.Now(),
			}:
			case <-s.closed:
				return
			}
		}
	}()

	return s, nil
}
