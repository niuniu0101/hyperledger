package mode

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/client/pkg/network"
	"github.com/hyperledger/client/pkg/pipeline"
	"github.com/hyperledger/consistent"
)

// RunPipelineMode 执行流水线并发查询模式
func RunPipelineMode(hrm *consistent.HashRingManager, serverClients map[string]*network.TCPClient, files []os.DirEntry, ringToServer map[int]string, centerServer string, workerCount int, verify bool, cacheBytes int64, flushTimeout time.Duration) {
	fmt.Println("=== 使用流水线模式 ===")
	fmt.Printf("每个存储服务器连接使用 %d 个worker goroutine处理响应\n", workerCount)

	// 构造本地的 fileHash -> 原始文件名/大小 映射
	hashToName := make(map[uint64]string, len(files))
	hashToSize := make(map[uint64]uint32, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		hash := sha256.Sum256([]byte(name))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		hashToName[fileHash] = name
		// 使用本地 cid_files 中的文件大小作为 size hint（若不可读则为0）
		if fi, err := os.Stat(filepath.Join("cid_files", name)); err == nil && fi.Mode().IsRegular() {
			sz := fi.Size()
			if sz < 0 {
				sz = 0
			}
			if sz > int64(^uint32(0)) {
				sz = int64(^uint32(0))
			}
			hashToSize[fileHash] = uint32(sz)
		} else {
			hashToSize[fileHash] = 0
		}
	}

	hashFunc := func(fileName string) (string, uint64) {
		hash := sha256.Sum256([]byte(fileName))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		ringID := fmt.Sprintf("ring%d", fileHash%4)

		shortName := fileName
		if len(shortName) > 16 {
			shortName = shortName[:16]
		}
		return hrm.LocateKey(ringID, shortName), fileHash
	}

	receiveDir := "file_receive"
	if err := os.MkdirAll(receiveDir, 0755); err != nil {
		log.Fatalf("创建接收目录失败: %v", err)
	}

	// 建立异步会话与结果收集
	sessions := make(map[string]*network.AsyncQuerySession)
	var expectedCount int64
	var receivedCount int64
	var recvWG sync.WaitGroup

	// 全局查询状态表：0-pending, 1-success, 2-failed
	const (
		statePending = int32(0)
		stateSuccess = int32(1)
		stateFailed  = int32(2)
	)
	var queryState sync.Map // map[uint64]int32

	setPendingIfAbsent := func(h uint64) bool {
		if _, loaded := queryState.Load(h); !loaded {
			queryState.Store(h, statePending)
			return true
		}
		return false
	}
	transitionFromPending := func(h uint64, to int32) bool {
		if v, ok := queryState.Load(h); ok {
			if vv, ok2 := v.(int32); ok2 && vv == statePending {
				queryState.Store(h, to)
				return true
			}
			return false
		}
		return false
	}

	ensureCounted := func(h uint64, to int32) bool {
		if v, ok := queryState.Load(h); ok {
			if vv, ok2 := v.(int32); ok2 {
				if vv == to {
					return false
				}
				queryState.Store(h, to)
				return true
			}
		}
		queryState.Store(h, to)
		return true
	}

	type fallbackTask struct {
		NodeName string
		FileHash uint64
	}
	fallbackCh := make(chan fallbackTask, 4096)
	doneCh := make(chan struct{})

	var centerServerPending int64
	var centerServerProcessed int64
	recvWG.Add(1)
	go func() {
		defer recvWG.Done()
		for {
			select {
			case t, ok := <-fallbackCh:
				if !ok {
					return
				}
				if sess, ok := sessions[centerServer]; ok && sess != nil {
					fmt.Printf("[回退ing] to %v\n", centerServer)
					size := uint32(0)
					if !sess.TryEnqueueWithSize(t.NodeName, t.FileHash, size) {
						go sess.EnqueueWithSize(t.NodeName, t.FileHash, size)
					}
					continue
				}
			case <-doneCh:
				return
			}
		}
	}()

	for ring := 0; ring <= 3; ring++ {
		addr, ok := ringToServer[ring]
		if !ok {
			continue
		}
		cli := serverClients[addr]
		if cli == nil {
			continue
		}
		// trustedRootLookup uses hrm cache first
		sess, err := cli.StartAsyncQueryWithWorkers(1024, workerCount, verify, func(node string) (string, error) {
			if verify {
				if v, ok := hrm.GetMerkleRootHash(node); ok {
					if len(v) >= 2 && (v[0:2] == "0x" || v[0:2] == "0X") {
						return v[2:], nil
					}
					return v, nil
				}
				return "", fmt.Errorf("no merkle root for node %s", node)
			}
			return "", nil
		})
		if err != nil {
			fmt.Printf("启动异步会话失败 %s: %v\n", addr, err)
			continue
		}
		sessions[addr] = sess
		sess.SetInFlightLimit(2048)

		// diagnostic: print that session was created
		fmt.Printf("[diag] session created for addr=%s workers=%d\n", addr, workerCount)
		recvWG.Add(1)
		go func(s *network.AsyncQuerySession, ringID int, serverAddr string) {
			defer recvWG.Done()
			for res := range s.Results() {
				if res.Err == nil && len(res.Data) > 0 {
					if outName, ok := hashToName[res.ReturnedHash]; ok {
						filePath := filepath.Join(receiveDir, outName)
						if err := os.WriteFile(filePath, res.Data, 0644); err != nil {
							fmt.Printf("[写入失败] ring=%d server=%s node=%s hash=%d 文件=%s err=%v\n", ringID, serverAddr, res.NodeName, res.ReturnedHash, outName, err)
							if transitionFromPending(res.RequestHash, stateFailed) {
								atomic.AddInt64(&receivedCount, 1)
							}
							select {
							case fallbackCh <- fallbackTask{NodeName: res.NodeName, FileHash: res.RequestHash}:
							case <-doneCh:
							default:
								fmt.Printf("[警告] 写入失败回退通道已满，跳过回退 node=%s hash=%d\n", res.NodeName, res.RequestHash)
							}
							continue
						}
						if ensureCounted(res.RequestHash, stateSuccess) {
							atomic.AddInt64(&receivedCount, 1)
						}
						fmt.Printf("[成功] ring=%d server=%s node=%s hash=%d 文件=%s\n", ringID, serverAddr, res.NodeName, res.ReturnedHash, outName)
						continue
					}
				}
				if ensureCounted(res.RequestHash, stateFailed) {
					atomic.AddInt64(&receivedCount, 1)
				}
				if res.Err != nil {
					fmt.Printf("[失败] ring=%d server=%s node=%s hash=%d err=%v -> 回退中心\n", ringID, serverAddr, res.NodeName, res.RequestHash, res.Err)
				} else if len(res.Data) == 0 {
					fmt.Printf("[失败] ring=%d server=%s node=%s hash=%d 未命中 -> 回退中心\n", ringID, serverAddr, res.NodeName, res.RequestHash)
				} else {
					fmt.Printf("[失败] ring=%d server=%s node=%s hash=%d 未匹配文件名 -> 回退中心\n", ringID, serverAddr, res.NodeName, res.RequestHash)
				}
				select {
				case fallbackCh <- fallbackTask{NodeName: res.NodeName, FileHash: res.RequestHash}:
				case <-doneCh:
				default:
					fmt.Printf("[警告] 回退通道已满，跳过回退 node=%s hash=%d\n", res.NodeName, res.RequestHash)
				}
			}
		}(sess, ring, addr)
	}

	queryPipeline := pipeline.NewClientPipeline(
		hashFunc,
		func(node string, tasks []*pipeline.FileTask) {
			if node == "" {
				for _, t := range tasks {
					if set := setPendingIfAbsent(t.FileHash); set {
						atomic.AddInt64(&expectedCount, 1)
					}
					if ensureCounted(t.FileHash, stateFailed) {
						atomic.AddInt64(&receivedCount, 1)
					}
					fmt.Printf("[流水线] 未找到文件 %s 的归属节点，标记为失败\n", t.FileName)
				}
				return
			}

			ringGroups := make(map[int][]*pipeline.FileTask)
			for _, t := range tasks {
				ring := int(t.FileHash % 4)
				ringGroups[ring] = append(ringGroups[ring], t)
			}

			for ring, group := range ringGroups {
				if len(group) == 0 {
					continue
				}
				serverAddr, ok := ringToServer[ring]
				if !ok {
					fmt.Printf("[流水线] ring%d 没有对应的服务器地址，跳过该组\n", ring)
					for _, t := range group {
						if set := setPendingIfAbsent(t.FileHash); set {
							atomic.AddInt64(&expectedCount, 1)
						}
						if ensureCounted(t.FileHash, stateFailed) {
							atomic.AddInt64(&receivedCount, 1)
						}
					}
					continue
				}
				sess, ok := sessions[serverAddr]
				if !ok || sess == nil {
					fmt.Printf("[流水线] 未找到服务器 %s 的异步会话，跳过该组\n", serverAddr)
					for _, t := range group {
						if set := setPendingIfAbsent(t.FileHash); set {
							atomic.AddInt64(&expectedCount, 1)
						}
						if ensureCounted(t.FileHash, stateFailed) {
							atomic.AddInt64(&receivedCount, 1)
						}
					}
					continue
				}
				for _, t := range group {
					if set := setPendingIfAbsent(t.FileHash); set {
						atomic.AddInt64(&expectedCount, 1)
					}
					sizeHint := uint32(0)
					if t.Size > 0 {
						sizeHint = uint32(t.Size)
					}
					if !sess.TryEnqueueWithSize(t.Node, t.FileHash, sizeHint) {
						inF, p, qLen, lim := sess.Metrics()
						fmt.Printf("[投递-回退] server=%s node=%s hash=%d 原因=sendCh满/限流 inFlight=%d pending=%d q=%d limit=%d\n", serverAddr, t.Node, t.FileHash, inF, p, qLen, lim)
						select {
						case fallbackCh <- fallbackTask{NodeName: t.Node, FileHash: t.FileHash}:
						case <-doneCh:
						default:
							fmt.Printf("[警告] 回退通道已满，跳过回退 node=%s hash=%d\n", t.Node, t.FileHash)
						}
						continue
					}
				}
			}
		},
		15,
	)
	// 使用配置中的超时与字节阈值
	if flushTimeout > 0 {
		queryPipeline.SetFlushTimeout(flushTimeout)
	}
	if cacheBytes > 0 {
		queryPipeline.SetDefaultCacheBytes(cacheBytes)
	}
	queryPipeline.SetSizeFunc(func(fileName string, fileHash uint64) int {
		if v, ok := hashToSize[fileHash]; ok {
			return int(v)
		}
		return 0
	})

	queryCount := 0
	fmt.Println("开始流水线查询文件...")
	for _, file := range files {
		if queryCount >= 5000 || file.IsDir() {
			continue
		}
		queryPipeline.PushWorkload(file.Name(), nil)
		queryCount++
	}

	// diagnostic: print session metrics after enqueueing workloads
	for addr, s := range sessions {
		if s == nil {
			fmt.Printf("[diag] session for %s is nil\n", addr)
			continue
		}
		inF, p, qLen, lim := s.Metrics()
		fmt.Printf("[diag] post-enqueue metrics addr=%s inFlight=%d pending=%d sendQ=%d limit=%d\n", addr, inF, p, qLen, lim)
	}

	queryPipeline.Wait()

	progressTicker := time.NewTicker(20 * time.Second)
	defer progressTicker.Stop()

	timeoutTimer := time.NewTimer(5 * time.Minute)
	defer timeoutTimer.Stop()

	lastProgressTime := time.Now()
	lastReceivedCount := int64(0)

	for {
		rc := atomic.LoadInt64(&receivedCount)
		ec := atomic.LoadInt64(&expectedCount)

		allCompleted := true
		totalFiles := int64(0)
		completedFiles := int64(0)

		queryState.Range(func(key, value interface{}) bool {
			totalFiles++
			if status, ok := value.(int32); ok {
				if status == stateSuccess || status == stateFailed {
					completedFiles++
				} else {
					allCompleted = false
				}
			} else {
				allCompleted = false
			}
			return true
		})

		if allCompleted && totalFiles > 0 && completedFiles == totalFiles {
			fmt.Printf("[完成] 所有请求已完成: 已接收=%d / 期望=%d (实际文件数=%d)\n", rc, ec, totalFiles)
			close(doneCh)
			for _, s := range sessions {
				if s != nil {
					s.Close()
				}
			}
			close(fallbackCh)
			break
		}

		select {
		case <-progressTicker.C:
			centerPending := atomic.LoadInt64(&centerServerPending)
			centerProcessed := atomic.LoadInt64(&centerServerProcessed)
			fmt.Printf("[进度] 已接收=%d / 期望=%d | 中心服务器: 待处理=%d 已处理=%d | 实际文件: 总数=%d 已完成=%d\n", rc, ec, centerPending, centerProcessed, totalFiles, completedFiles)

			if rc > lastReceivedCount {
				lastProgressTime = time.Now()
				lastReceivedCount = rc
			} else if time.Since(lastProgressTime) > 30*time.Second {
				fmt.Printf("[警告] 超过30秒没有进展，当前状态: 已接收=%d / 期望=%d\n", rc, ec)
				pendingCount := 0
				successCount := 0
				failedCount := 0
				unknownCount := 0
				queryState.Range(func(key, value interface{}) bool {
					if status, ok := value.(int32); ok {
						switch status {
						case statePending:
							pendingCount++
						case stateSuccess:
							successCount++
						case stateFailed:
							failedCount++
						default:
							unknownCount++
						}
					} else {
						unknownCount++
					}
					return true
				})
				fmt.Printf("[调试] queryState状态: pending=%d success=%d failed=%d unknown=%d\n", pendingCount, successCount, failedCount, unknownCount)
				fmt.Printf("[调试] 回退通道状态: 长度=%d 容量=%d\n", len(fallbackCh), cap(fallbackCh))
				lastProgressTime = time.Now()
			}

			for ring := 0; ring <= 3; ring++ {
				addr, ok := ringToServer[ring]
				if !ok {
					continue
				}
				if s, ok := sessions[addr]; ok && s != nil {
					inF, p, qLen, lim := s.Metrics()
					fmt.Printf("[进度-服务器] ring=%d addr=%s inFlight=%d pending=%d q=%d limit=%d\n", ring, addr, inF, p, qLen, lim)
				}
			}

		case <-timeoutTimer.C:
			fmt.Printf("[超时] 5分钟超时，强制结束: 已接收=%d / 期望=%d\n", rc, ec)
			goto timeoutExit

		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

timeoutExit:
	recvWG.Wait()

	finalReceived := atomic.LoadInt64(&receivedCount)
	finalExpected := atomic.LoadInt64(&expectedCount)
	centerProcessed := atomic.LoadInt64(&centerServerProcessed)

	successCount := int64(0)
	failedCount := int64(0)
	queryState.Range(func(key, value interface{}) bool {
		if status, ok := value.(int32); ok {
			switch status {
			case stateSuccess:
				successCount++
			case stateFailed:
				failedCount++
			}
		}
		return true
	})

	fmt.Printf("=== 查询流水线完成统计 ===\n")
	fmt.Printf("总查询文件数: %d\n", queryCount)
	fmt.Printf("期望接收数: %d\n", finalExpected)
	fmt.Printf("实际接收数: %d\n", finalReceived)
	fmt.Printf("成功文件数: %d\n", successCount)
	fmt.Printf("失败文件数: %d\n", failedCount)
	fmt.Printf("中心服务器处理数: %d\n", centerProcessed)
	fmt.Printf("缺失文件数: %d\n", finalExpected-finalReceived)

	if actualFiles, err := os.ReadDir(receiveDir); err == nil {
		fmt.Printf("实际接收目录文件数: %d\n", len(actualFiles))
		fileCounts := make(map[string]int)
		for _, file := range actualFiles {
			if !file.IsDir() {
				fileCounts[file.Name()]++
			}
		}
		duplicateCount := 0
		for _, count := range fileCounts {
			if count > 1 {
				duplicateCount += count - 1
			}
		}
		if duplicateCount > 0 {
			fmt.Printf("重复文件数: %d\n", duplicateCount)
		}
		totalSize := int64(0)
		for _, file := range actualFiles {
			if !file.IsDir() {
				if info, err := file.Info(); err == nil {
					totalSize += info.Size()
				}
			}
		}
		fmt.Printf("接收文件总大小: %d 字节 (%.2f MB)\n", totalSize, float64(totalSize)/(1024*1024))
	} else {
		fmt.Printf("无法读取接收目录: %v\n", err)
	}
	fmt.Printf("========================\n")
}
