package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sync"
	"sync/atomic"

	"github.com/hyperledger/client/pkg/fabric"
	"github.com/hyperledger/client/pkg/models"
	"github.com/hyperledger/client/pkg/network"
	"github.com/hyperledger/client/pkg/pipeline"
	"github.com/hyperledger/consistent"
)

// ...existing code...

const centerServer = "10.0.0.185:8082"

/* var ringToServer = map[int]string{
	0: "8.208.81.144:8082",
	1: "47.237.16.182:8082",
	4: "47.91.124.37:8082",
	5: "47.91.124.37:8082",
	2: "8.220.202.165:8082",
	3: "localhost:8082",
} */

/* var ringToServer = map[int]string{
	0: "10.0.0.185:8082",
	1: "10.0.0.185:8082",
	4: "10.0.0.185:8082",
	5: "10.0.0.185:8082",
	2: "10.0.0.185:8082",
	3: "10.0.0.185:8082",
} */

var ringToServer = map[int]string{
	0: "localhost:8082",
	1: "10.0.0.185:8082",
	4: "localhost:8082",
	5: "localhost:8082",
	2: "10.0.0.186:8082",
	3: "10.0.0.187:8082",
}

/* var ringToServer = map[int]string{
	0: "47.251.95.138:8082",
	1: "47.251.95.138:8082",
	4: "47.251.95.138:8082",
	5: "localhost:8082",
	2: "47.251.95.138:8082",
	3: "47.251.95.138:8082",
}
*/
/*
环1（中心服务器）：47.251.95.138
环0：47.251.71.236
环2：47.251.117.139
环3：47.88.26.4
*/
var usepipeline int
var workerCount int

func addNodesToBlockchain(fabricClient *fabric.FabricClient) {
	for i := 0; i < 40; i++ {
		serverName := fmt.Sprintf("node%d", i)
		ringID := fmt.Sprintf("ring%d", i%4)
		for {
			err := fabricClient.AddServerNode(ringID, serverName, "")
			if err != nil {
				fmt.Println("添加节点失败: %s, err: %v", serverName, err)
				continue
			}
			break
		}
		fmt.Printf("add %s to %s\n", serverName, ringID)
	}
	/*
		 	for i := 40; i < 60; i++ {
				serverName := fmt.Sprintf("node%d", i)
				ringID := fmt.Sprintf("ring%d", 4+i%2)
				for {
					err := fabricClient.AddServerNode(ringID, serverName, "")
					if err != nil {
						fmt.Println("添加节点失败: %s, err: %v", serverName, err)
						continue
					}
					break
				}
				fmt.Printf("add %s to %s\n", serverName, ringID)
			}
	*/
}

func buildLocalHashRing(fabricClient *fabric.FabricClient) (*consistent.HashRingManager, error) {
	data, err := fabricClient.GetAllRingsData()
	if err != nil {
		return nil, fmt.Errorf("获取区块链哈希环失败: %v", err)
	}
	var container models.AllRingsContainer
	if err := json.Unmarshal(data, &container); err != nil {
		return nil, fmt.Errorf("解析哈希环数据失败: %v", err)
	}
	hrm := consistent.NewHashRingManager()
	hrm.BuildHashRingsFromData(&container)
	return hrm, nil
}

func displayRingStatus(fabricClient *fabric.FabricClient, title string) {
	fmt.Println("---------------------------------------")
	fmt.Printf("[状态检查] %s\n", title)
	hrm, err := buildLocalHashRing(fabricClient)
	if err != nil {
		log.Fatalf("构建本地哈希环失败: %v", err)
	}
	for ringID, ring := range hrm.GetAllRings() {
		members := ring.GetMembers()
		if len(members) > 0 {
			fmt.Printf("Ring %s 上的节点: ", ringID)
			for _, node := range members {
				fmt.Printf("%s ", node.String())
			}
			fmt.Println()
		} else {
			fmt.Printf("Ring %s 上的节点: (空)\n", ringID)
		}
	}
	fmt.Println("---------------------------------------")
}

func locateFileNode(hrm *consistent.HashRingManager, ringID, fileName string) string {
	return hrm.LocateKey(ringID, fileName)
}

func testRouteTime(hrm *consistent.HashRingManager, fileNames []string) {
	outputFile, err := os.Create("route_time_log.txt")
	if err != nil {
		log.Fatalf("创建路由时间日志文件失败: %v", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	fmt.Fprintf(writer, "序号\t文件名\t环ID\t节点\t路由耗时(ns)\n")

	start := time.Now()
	success := 0

	for i, name := range fileNames {
		routeStart := time.Now()

		hash := sha256.Sum256([]byte(name))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		ringID := fmt.Sprintf("ring%d", fileHash%4)
		node := locateFileNode(hrm, ringID, name)

		routeDuration := time.Since(routeStart)

		if node != "" {
			success++
			fmt.Fprintf(writer, "%d\t%s\t%s\t%s\t%d\n",
				i+1, name, ringID, node, routeDuration.Nanoseconds())
		} else {
			fmt.Fprintf(writer, "%d\t%s\t%s\t未找到\t%d\n",
				i+1, name, ringID, routeDuration.Nanoseconds())
		}
	}

	dur := time.Since(start)

	fmt.Fprintf(writer, "\n=== 统计信息 ===\n")
	fmt.Fprintf(writer, "总文件数: %d\n", len(fileNames))
	fmt.Fprintf(writer, "成功路由: %d\n", success)
	fmt.Fprintf(writer, "总耗时: %v\n", dur)

	if success > 0 {
		avg := dur / time.Duration(success)
		fmt.Fprintf(writer, "平均耗时: %v\n", avg)

		fmt.Printf("[路由测试] 总数: %d, 成功: %d, 总耗时: %v, 平均耗时: %v\n",
			len(fileNames), success, dur, avg)
		fmt.Printf("[路由测试] 详细日志已保存到: route_time_log.txt\n")
	} else {
		fmt.Println("[路由测试] 未能成功路由任何文件名！")
	}
}

func prepareServerClients(centerClient *network.TCPClient) (map[string]*network.TCPClient, error) {
	clientCache := make(map[string]*network.TCPClient, len(ringToServer)+1)

	if err := centerClient.Connect(); err != nil {
		return nil, fmt.Errorf("中心服务器连接失败: %w", err)
	}
	clientCache[centerServer] = centerClient

	for ring := 0; ring <= 3; ring++ {
		addr, ok := ringToServer[ring]
		if !ok {
			continue
		}
		if _, exists := clientCache[addr]; exists {
			continue
		}
		if addr == centerServer {
			clientCache[addr] = centerClient
			continue
		}
		client := network.NewTCPClient(addr)
		if err := client.Connect(); err != nil {
			return nil, fmt.Errorf("连接 ring%d 服务器(%s) 失败: %w", ring, addr, err)
		}
		clientCache[addr] = client
	}
	return clientCache, nil
}

func closeServerClients(cache map[string]*network.TCPClient) {
	closed := make(map[*network.TCPClient]struct{})
	for addr, client := range cache {
		if client == nil {
			continue
		}
		if _, ok := closed[client]; ok {
			continue
		}
		if err := client.Close(); err != nil {
			fmt.Println("关闭服务器连接失败(%s): %v", addr, err)
		}
		closed[client] = struct{}{}
	}
}

func runSerialMode(hrm *consistent.HashRingManager, centerClient *network.TCPClient, serverClients map[string]*network.TCPClient, files []os.DirEntry) {
	fmt.Println("=== 使用串行模式 ===")

	uploadCount := 0
	fmt.Println("开始串行上传文件...")
	for _, file := range files {
		if uploadCount >= 5000 || file.IsDir() {
			continue
		}

		filePath := filepath.Join("./cid_files", file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Println("读取文件失败: %s, err: %v", filePath, err)
			continue
		}

		hash := sha256.Sum256([]byte(file.Name()))

		fileHash := binary.BigEndian.Uint64(hash[:8])
		// log.Println("文件: %s, Hash: %d", file.Name(), fileHash)

		ringID := fmt.Sprintf("ring%d", fileHash%4)

		fileName := file.Name()
		if len(fileName) > 16 {
			fileName = fileName[:16]
		}
		nodeName := locateFileNode(hrm, ringID, fileName)

		if nodeName == "" {
			fmt.Println("未找到文件 %s 的归属节点，跳过", fileName)
			continue
		}

		if err := centerClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
			fmt.Println("串行上传失败: %s, err: %v", fileName, err)
		} else {
			fmt.Println("串行上传成功: %s -> %s (%s)", fileName, nodeName, ringID)
		}
		uploadCount++
	}
	fmt.Printf("串行上传完成，共上传 %d 个文件\n", uploadCount)
	select {}

	// uploadCount := 0
	// fmt.Println("开始串行上传文件...")
	// for _, file := range files {
	// 	if uploadCount >= 1000 || file.IsDir() {
	// 		continue
	// 	}

	// 	filePath := filepath.Join("./files", file.Name())
	// 	data, err := os.ReadFile(filePath)
	// 	if err != nil {
	// 		fmt.Println("读取文件失败: %s, err: %v", filePath, err)
	// 		continue
	// 	}

	// 	hash := sha256.Sum256([]byte(file.Name()))
	// 	fileHash := binary.BigEndian.Uint64(hash[:8])
	// 	ringID := fmt.Sprintf("ring%d", fileHash%4)

	// 	fileName := file.Name()
	// 	if len(fileName) > 16 {
	// 		fileName = fileName[:16]
	// 	}
	// 	nodeName := locateFileNode(hrm, ringID, fileName)

	// 	if nodeName == "" {
	// 		fmt.Println("未找到文件 %s 的归属节点，跳过", fileName)
	// 		continue
	// 	}

	// 	// 根据环ID找到对应的服务器地址
	// 	ring := int(fileHash % 4)
	// 	serverAddr, ok := ringToServer[ring]
	// 	if !ok {
	// 		fmt.Println("环%d没有对应的服务器地址，使用中心服务器: %s", ring, fileName)
	// 		serverAddr = centerServer
	// 	}

	// 	// 获取对应服务器的客户端连接
	// 	targetClient, exists := serverClients[serverAddr]
	// 	if !exists || targetClient == nil {
	// 		fmt.Println("未找到服务器 %s 的连接，跳过: %s", serverAddr, fileName)
	// 		continue
	// 	}

	// 	// 直接上传到目标服务器
	// 	if err := targetClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
	// 		fmt.Println("直接上传失败: %s -> %s (%s), err: %v", fileName, nodeName, serverAddr, err)

	// 		// 如果直接上传失败，回退到中心服务器
	// 		fmt.Println("回退到中心服务器上传: %s", fileName)
	// 		if err := centerClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
	// 			fmt.Println("中心服务器上传失败: %s, err: %v", fileName, err)
	// 		} else {
	// 			fmt.Println("中心服务器上传成功: %s -> %s", fileName, nodeName)
	// 			uploadCount++
	// 		}
	// 	} else {
	// 		fmt.Println("直接上传成功: %s -> %s (%s)", fileName, nodeName, serverAddr)
	// 		uploadCount++
	// 	}
	// }
	// fmt.Printf("串行上传完成，共上传 %d 个文件\n", uploadCount)

	// // 保持程序运行，等待其他操作
	// select {}

	// receiveDir := "file_receive"

	// if err := os.MkdirAll(receiveDir, 0755); err != nil {
	// 	log.Fatalf("创建接收目录失败: %v", err)
	// }

	// // 构造本地的 fileHash -> 原始文件名 映射
	// hashToName := make(map[uint64]string, len(files))

	// for _, f := range files {
	// 	if f.IsDir() {
	// 		continue
	// 	}
	// 	name := f.Name()
	// 	hash := sha256.Sum256([]byte(name))
	// 	fileHash := binary.BigEndian.Uint64(hash[:8])
	// 	hashToName[fileHash] = name
	// }

	// queryCount := 0
	// fmt.Println("开始串行查询文件...")

	// for _, file := range files {
	// 	if queryCount >= 1000 || file.IsDir() {
	// 		continue
	// 	}

	// 	hash := sha256.Sum256([]byte(file.Name()))
	// 	fileHash := binary.BigEndian.Uint64(hash[:8])
	// 	ring := int(fileHash % 4)
	// 	ringID := fmt.Sprintf("ring%d", ring)

	// 	fileName := file.Name()
	// 	if len(fileName) > 16 {
	// 		fileName = fileName[:16]
	// 	}
	// 	nodeName := locateFileNode(hrm, ringID, fileName)

	// 	if nodeName == "" {
	// 		fmt.Println("未找到文件 %s 的归属节点，跳过", fileName)
	// 		continue
	// 	}

	// 	serverAddr, ok := ringToServer[ring]
	// 	if !ok {
	// 		fmt.Println("ring%d 没有对应的服务器地址，使用中心服务器: %s", ring, file.Name())
	// 		serverAddr = centerServer
	// 	}
	// 	queryClient, exists := serverClients[serverAddr]
	// 	if !exists || queryClient == nil {
	// 		fmt.Println("未找到服务器 %s 的专属连接，跳过: %s", serverAddr, file.Name())
	// 		continue
	// 	}

	// 	returnedHash, data, err := queryClient.QueryFile(nodeName, fileHash)

	// 	if err != nil || len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
	// 		fmt.Println("%d", data)
	// 		fmt.Println("  客户端: %s", queryClient.Conn.RemoteAddr().String())
	// 		fmt.Println("  服务器: %s", queryClient.Conn.LocalAddr().String())
	// 		fmt.Println("从 %s 查询失败，回退到中心服务器: %s", serverAddr, file.Name())
	// 		returnedHash, data, err = centerClient.QueryFile(nodeName, fileHash)
	// 		if err != nil {
	// 			fmt.Println("中心服务器查询失败: %s, err: %v", file.Name(), err)
	// 			continue
	// 		}
	// 	}

	// 	if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
	// 		fmt.Println("服务端返回错误: %s, msg: %s", file.Name(), string(data))
	// 		continue
	// 	}

	// 	// 使用服务器返回的 hash 查找本地原始文件名（fallback 使用当前文件名）
	// 	outName, ok := hashToName[returnedHash]
	// 	if !ok {
	// 		outName = file.Name()
	// 	}

	// 	outPath := filepath.Join(receiveDir, outName)
	// 	if err := os.WriteFile(outPath, data, 0644); err != nil {
	// 		fmt.Println("写入文件失败: %s, err: %v", outPath, err)
	// 	} else {
	// 		fmt.Println("串行查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
	// 	}
	// 	queryCount++
	// }

	// fmt.Printf("串行查询完成，共查询 %d 个文件\n", queryCount)

	receiveDir := "file_receive"

	if err := os.MkdirAll(receiveDir, 0755); err != nil {
		log.Fatalf("创建接收目录失败: %v", err)
	}

	// 创建日志文件
	logFile, err := os.OpenFile("query_results.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("创建日志文件失败: %v", err)
	}
	defer logFile.Close()

	// 创建带格式的logger
	logger := log.New(logFile, "", log.LstdFlags)

	// 构造本地的 fileHash -> 原始文件名 映射
	hashToName := make(map[uint64]string, len(files))

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		hash := sha256.Sum256([]byte(name))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		hashToName[fileHash] = name
	}

	queryCount := 0
	successCount := 0
	failCount := 0
	fmt.Println("开始串行查询文件...")

	for _, file := range files {
		if queryCount >= 5000 || file.IsDir() {
			continue
		}

		hash := sha256.Sum256([]byte(file.Name()))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		logger.Println("file: %s ,hash: %d", file.Name(), fileHash)
		ring := int(fileHash % 4)
		ringID := fmt.Sprintf("ring%d", ring)

		fileName := file.Name()
		if len(fileName) > 16 {
			fileName = fileName[:16]
		}
		nodeName := locateFileNode(hrm, ringID, fileName)

		if nodeName == "" {
			logMsg := fmt.Sprintf("未找到文件 %s 的归属节点，跳过", fileName)
			fmt.Println(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
			continue
		}

		serverAddr, ok := ringToServer[ring]
		if !ok {
			logMsg := fmt.Sprintf("ring%d 没有对应的服务器地址，使用中心服务器: %s", ring, file.Name())
			fmt.Println(logMsg)
			logger.Printf("WARN: %s", logMsg)
			serverAddr = centerServer
		}
		queryClient, exists := serverClients[serverAddr]
		if !exists || queryClient == nil {
			logMsg := fmt.Sprintf("未找到服务器 %s 的专属连接，跳过: %s", serverAddr, file.Name())
			fmt.Println(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
			continue
		}

		returnedHash, data, err := queryClient.QueryFile(nodeName, fileHash)

		if err != nil || len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			fmt.Println("%d", data)
			fmt.Println("  客户端: %s", queryClient.Conn.RemoteAddr().String())
			fmt.Println("  服务器: %s", queryClient.Conn.LocalAddr().String())
			logMsg := fmt.Sprintf("从 %s 查询失败，回退到中心服务器: %s", serverAddr, file.Name())
			fmt.Println(logMsg)
			logger.Printf("FAIL: %s", logMsg)

			returnedHash, data, err = centerClient.QueryFile(nodeName, fileHash)
			if err != nil {
				logMsg := fmt.Sprintf("中心服务器查询失败: %s, err: %v", file.Name(), err)
				fmt.Println(logMsg)
				logger.Printf("FAIL: %s", logMsg)
				failCount++
				continue
			}
		}

		if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			logMsg := fmt.Sprintf("服务端返回错误: %s, msg: %s", file.Name(), string(data))
			fmt.Println(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
			continue
		}

		// 使用服务器返回的 hash 查找本地原始文件名（fallback 使用当前文件名）
		outName, ok := hashToName[returnedHash]
		if !ok {
			outName = file.Name()
		}

		outPath := filepath.Join(receiveDir, outName)
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			logMsg := fmt.Sprintf("写入文件失败: %s, err: %v", outPath, err)
			fmt.Println(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
		} else {
			logMsg := fmt.Sprintf("串行查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
			fmt.Println(logMsg)
			logger.Printf("SUCCESS: %s", logMsg)
			successCount++
		}
		queryCount++
	}

	// 写入统计信息
	summary := fmt.Sprintf("串行查询完成，共查询 %d 个文件，成功 %d 个，失败 %d 个", queryCount, successCount, failCount)
	fmt.Println(summary)
	logger.Printf("SUMMARY: %s", summary)

}

func runPipelineMode(hrm *consistent.HashRingManager, centerClient *network.TCPClient, serverClients map[string]*network.TCPClient, files []os.DirEntry) {
	fmt.Println("=== 使用流水线模式 ===")
	fmt.Printf("每个存储服务器连接使用 %d 个worker goroutine处理响应\n", workerCount)

	// // 构造本地的 fileHash -> 原始文件名 映射
	hashToName := make(map[uint64]string, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		hash := sha256.Sum256([]byte(name))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		hashToName[fileHash] = name
	}

	hashFunc := func(fileName string) (string, uint64) {
		hash := sha256.Sum256([]byte(fileName))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		ringID := fmt.Sprintf("ring%d", fileHash%4)

		shortName := fileName
		if len(shortName) > 16 {
			shortName = shortName[:16]
		}
		return locateFileNode(hrm, ringID, shortName), fileHash
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
			// 如果已经是success或failed，说明已经处理过了，不应该再次计数
			return false
		}
		// 未存在则不算一次有效完成
		return false
	}

	// 新增：确保文件被计数的函数
	ensureCounted := func(h uint64, to int32) bool {
		if v, ok := queryState.Load(h); ok {
			if vv, ok2 := v.(int32); ok2 {
				// 如果已经是目标状态，说明已经计数过了
				if vv == to {
					return false
				}
				// 更新状态
				queryState.Store(h, to)
				return true
			}
		}
		// 如果不存在，创建并计数
		queryState.Store(h, to)
		return true
	}
	// 回退任务通道与回退线程
	type fallbackTask struct {
		NodeName string
		FileHash uint64
	}
	fallbackCh := make(chan fallbackTask, 4096)
	doneCh := make(chan struct{}) // 用于优雅关闭

	// 中心服务器状态监控
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
				// 优先把回退请求发到中心服务器的异步会话发送队列（如果存在）
				if sess, ok := sessions[centerServer]; ok && sess != nil {
					fmt.Println("[回退ing] to %v", centerServer)
					// 尝试非阻塞入队，若失败则异步入队保证最终发送
					if !sess.TryEnqueue(t.NodeName, t.FileHash) {
						go sess.Enqueue(t.NodeName, t.FileHash)
					}
					// 交由中心会话的结果处理器计数/写文件
					continue
				}
				/*
					// 检查是否已经处理过
					if actual, loaded := queryState.Load(t.FileHash); loaded && (actual == stateSuccess || actual == stateFailed) {
						// 已经处理过，但仍然计入中心服务器处理数
						atomic.AddInt64(&centerServerProcessed, 1)
						continue
					}

					atomic.AddInt64(&centerServerPending, 1)
					fmt.Println("[回退] 中心服务器查询开始 node=%s hash=%d (待处理:%d)", t.NodeName, t.FileHash, atomic.LoadInt64(&centerServerPending))
					retHash, data, err := centerClient.QueryFile(t.NodeName, t.FileHash)
					atomic.AddInt64(&centerServerPending, -1)
					atomic.AddInt64(&centerServerProcessed, 1)

					if err != nil || (len(data) >= 5 && string(data[:5]) == "ERROR") {
						if err != nil {
							fmt.Println("[回退-失败] node=%s hash=%d err=%v (已处理:%d)", t.NodeName, t.FileHash, err, atomic.LoadInt64(&centerServerProcessed))
						} else {
							fmt.Println("[回退-失败] node=%s hash=%d msg=%q (已处理:%d)", t.NodeName, t.FileHash, string(data), atomic.LoadInt64(&centerServerProcessed))
						}
						if ensureCounted(t.FileHash, stateFailed) {
							atomic.AddInt64(&receivedCount, 1)
						}
						continue
					}
					if len(data) == 0 {
						// 中心也未命中：将状态置为失败（若仍为pending），并结束
						fmt.Println("[回退-未命中] node=%s hash=%d (已处理:%d)", t.NodeName, t.FileHash, atomic.LoadInt64(&centerServerProcessed))
						if ensureCounted(t.FileHash, stateFailed) {
							atomic.AddInt64(&receivedCount, 1)
						}
						continue
					}
					outName, ok := hashToName[retHash]
					if !ok {
						fmt.Println("[回退-警告] 未找到文件名映射 hash=%d (已处理:%d)", retHash, atomic.LoadInt64(&centerServerProcessed))
						if ensureCounted(t.FileHash, stateFailed) {
							atomic.AddInt64(&receivedCount, 1)
						}
						continue
					}
					filePath := filepath.Join(receiveDir, outName)
					if err := os.WriteFile(filePath, data, 0644); err != nil {
						fmt.Println("[回退-写入失败] 文件=%s hash=%d err=%v (已处理:%d)", outName, retHash, err, atomic.LoadInt64(&centerServerProcessed))
						if ensureCounted(t.FileHash, stateFailed) {
							atomic.AddInt64(&receivedCount, 1)
						}
						continue
					}
					if ensureCounted(t.FileHash, stateSuccess) {
						atomic.AddInt64(&receivedCount, 1)
					}
					fmt.Println("[回退-成功] 写入文件 %s (hash=%d) (已处理:%d)", outName, retHash, atomic.LoadInt64(&centerServerProcessed))
				*/
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
		// 使用多worker模式启动异步会话
		sess, err := cli.StartAsyncQueryWithWorkers(1024, workerCount)
		if err != nil {
			fmt.Println("启动异步会话失败 %s: %v", addr, err)
			continue
		}
		sessions[addr] = sess
		// 设置每台服务器的最大并发请求上限，避免长队导致超时
		sess.SetInFlightLimit(2048)
		recvWG.Add(1)
		go func(s *network.AsyncQuerySession, ringID int, serverAddr string) {
			defer recvWG.Done()
			for res := range s.Results() {
				if res.Err == nil && len(res.Data) > 0 {
					// 成功
					if outName, ok := hashToName[res.ReturnedHash]; ok {
						filePath := filepath.Join(receiveDir, outName)
						if err := os.WriteFile(filePath, res.Data, 0644); err != nil {
							fmt.Println("[写入失败] ring=%d server=%s node=%s hash=%d 文件=%s err=%v", ringID, serverAddr, res.NodeName, res.ReturnedHash, outName, err)
							if transitionFromPending(res.RequestHash, stateFailed) {
								atomic.AddInt64(&receivedCount, 1)
							}
							// 写入失败也要回退
							select {
							case fallbackCh <- fallbackTask{NodeName: res.NodeName, FileHash: res.RequestHash}:
							case <-doneCh:
								// 已关闭，跳过
							default:
								fmt.Println("[警告] 写入失败回退通道已满，跳过回退 node=%s hash=%d", res.NodeName, res.RequestHash)
							}
							continue
						}
						if ensureCounted(res.RequestHash, stateSuccess) {
							atomic.AddInt64(&receivedCount, 1)
						}
						fmt.Println("[成功] ring=%d server=%s node=%s hash=%d 文件=%s", ringID, serverAddr, res.NodeName, res.ReturnedHash, outName)
						continue
					}
				}
				// 未命中/失败：标记失败并回退（仅第一次从pending转失败时计入完成）
				if ensureCounted(res.RequestHash, stateFailed) {
					atomic.AddInt64(&receivedCount, 1)
				}
				if res.Err != nil {
					fmt.Println("[失败] ring=%d server=%s node=%s hash=%d err=%v -> 回退中心", ringID, serverAddr, res.NodeName, res.RequestHash, res.Err)
				} else if len(res.Data) == 0 {
					fmt.Println("[失败] ring=%d server=%s node=%s hash=%d 未命中 -> 回退中心", ringID, serverAddr, res.NodeName, res.RequestHash)
				} else {
					fmt.Println("[失败] ring=%d server=%s node=%s hash=%d 未匹配文件名 -> 回退中心", ringID, serverAddr, res.NodeName, res.RequestHash)
				}
				select {
				case fallbackCh <- fallbackTask{NodeName: res.NodeName, FileHash: res.RequestHash}:
					// 成功发送
				case <-doneCh:
					// 已关闭，跳过
				default:
					// 通道已满，记录日志但不panic
					fmt.Println("[警告] 回退通道已满，跳过回退 node=%s hash=%d", res.NodeName, res.RequestHash)
				}
			}
		}(sess, ring, addr)
	}

	queryPipeline := pipeline.NewClientPipeline(
		hashFunc,
		func(node string, tasks []*pipeline.FileTask) {
			// 如果无法定位节点，则将这些任务直接标记为失败，并计入期望与已接收
			if node == "" {
				for _, t := range tasks {
					if set := setPendingIfAbsent(t.FileHash); set {
						atomic.AddInt64(&expectedCount, 1)
					}
					if ensureCounted(t.FileHash, stateFailed) {
						atomic.AddInt64(&receivedCount, 1)
					}
					fmt.Println("[流水线] 未找到文件 %s 的归属节点，标记为失败", t.FileName)
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
					fmt.Println("[流水线] ring%d 没有对应的服务器地址，跳过该组", ring)
					// 这些任务也应被计入期望并标记失败以避免阻塞
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
					fmt.Println("[流水线] 未找到服务器 %s 的异步会话，跳过该组", serverAddr)
					// 同样计入期望并标记失败
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
					if !sess.TryEnqueue(t.Node, t.FileHash) {
						// 非阻塞投递失败（sendCh 满或 in-flight 限流），直接回退中心
						inF, p, qLen, lim := sess.Metrics()
						fmt.Println("[投递-回退] server=%s node=%s hash=%d 原因=sendCh满/限流 inFlight=%d pending=%d q=%d limit=%d", serverAddr, t.Node, t.FileHash, inF, p, qLen, lim)
						// 投递失败时，回退中心处理（完成状态将在回退线程中计数）
						select {
						case fallbackCh <- fallbackTask{NodeName: t.Node, FileHash: t.FileHash}:
						case <-doneCh:
						default:
							fmt.Println("[警告] 回退通道已满，跳过回退 node=%s hash=%d", t.Node, t.FileHash)
						}
						continue
					}
				}
			}
		},
		15,
	)
	queryPipeline.SetFlushTimeout(1 * time.Second)

	queryCount := 0
	fmt.Println("开始流水线查询文件...")
	for _, file := range files {
		if queryCount >= 1000 || file.IsDir() {
			continue
		}
		queryPipeline.PushWorkload(file.Name(), nil)
		queryCount++
	}
	queryPipeline.Wait()
	// 等全部响应回收，定期打印进度与每台服务器指标
	progressTicker := time.NewTicker(20 * time.Second)
	defer progressTicker.Stop()

	// 添加强制超时机制
	timeoutTimer := time.NewTimer(5 * time.Minute)
	defer timeoutTimer.Stop()

	lastProgressTime := time.Now()
	lastReceivedCount := int64(0)

	for {
		rc := atomic.LoadInt64(&receivedCount)
		ec := atomic.LoadInt64(&expectedCount)

		// 检查是否真正完成：所有文件都有最终状态
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
			fmt.Println("[完成] 所有请求已完成: 已接收=%d / 期望=%d (实际文件数=%d)", rc, ec, totalFiles)
			// 先关闭doneCh，通知所有goroutine停止
			close(doneCh)
			// 然后关闭所有会话
			for _, s := range sessions {
				if s != nil {
					s.Close()
				}
			}
			// 最后关闭fallbackCh
			close(fallbackCh)
			break
		}

		select {
		case <-progressTicker.C:
			centerPending := atomic.LoadInt64(&centerServerPending)
			centerProcessed := atomic.LoadInt64(&centerServerProcessed)
			fmt.Println("[进度] 已接收=%d / 期望=%d | 中心服务器: 待处理=%d 已处理=%d | 实际文件: 总数=%d 已完成=%d", rc, ec, centerPending, centerProcessed, totalFiles, completedFiles)

			// 检查是否有进展
			if rc > lastReceivedCount {
				lastProgressTime = time.Now()
				lastReceivedCount = rc
			} else if time.Since(lastProgressTime) > 30*time.Second {
				fmt.Println("[警告] 超过30秒没有进展，当前状态: 已接收=%d / 期望=%d", rc, ec)

				// 详细分析queryState状态
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
				fmt.Println("[调试] queryState状态: pending=%d success=%d failed=%d unknown=%d", pendingCount, successCount, failedCount, unknownCount)
				fmt.Println("[调试] 回退通道状态: 长度=%d 容量=%d", len(fallbackCh), cap(fallbackCh))

				lastProgressTime = time.Now() // 重置计时器
			}

			// 按服务器输出 inFlight/pending/队列
			for ring := 0; ring <= 3; ring++ {
				addr, ok := ringToServer[ring]
				if !ok {
					continue
				}
				if s, ok := sessions[addr]; ok && s != nil {
					inF, p, qLen, lim := s.Metrics()
					fmt.Println("[进度-服务器] ring=%d addr=%s inFlight=%d pending=%d q=%d limit=%d", ring, addr, inF, p, qLen, lim)
				}
			}

		case <-timeoutTimer.C:
			fmt.Println("[超时] 5分钟超时，强制结束: 已接收=%d / 期望=%d", rc, ec)
			goto timeoutExit

		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

timeoutExit:
	// 等待所有goroutine完成
	recvWG.Wait()

	// 统计最终结果
	finalReceived := atomic.LoadInt64(&receivedCount)
	finalExpected := atomic.LoadInt64(&expectedCount)
	centerProcessed := atomic.LoadInt64(&centerServerProcessed)

	// 统计成功和失败的文件数量
	successCount := int64(0)
	failedCount := int64(0)
	queryState.Range(func(key, value interface{}) bool {
		if status, ok := value.(int32); ok {
			if status == stateSuccess {
				successCount++
			} else if status == stateFailed {
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

	// 统计实际接收目录中的文件数量
	if actualFiles, err := os.ReadDir(receiveDir); err == nil {
		fmt.Printf("实际接收目录文件数: %d\n", len(actualFiles))

		// 检查是否有重复文件
		fileCounts := make(map[string]int)
		for _, file := range actualFiles {
			if !file.IsDir() {
				fileCounts[file.Name()]++
			}
		}

		// 统计重复文件
		duplicateCount := 0
		for _, count := range fileCounts {
			if count > 1 {
				duplicateCount += count - 1
			}
		}
		if duplicateCount > 0 {
			fmt.Printf("重复文件数: %d\n", duplicateCount)
		}

		// 统计文件大小
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

func annotateUnifiedData(hrm *consistent.HashRingManager, inputPath, outputPath string) error {
	inFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("打开输入文件失败: %w", err)
	}
	defer inFile.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.Split(line, "\t")
		if len(fields) == 0 {
			continue
		}

		fileName := fields[0]
		size := ""
		if len(fields) > 1 {
			size = fields[1]
		}

		hash := sha256.Sum256([]byte(fileName))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		ringID := fmt.Sprintf("ring%d", fileHash%4)

		shortName := fileName
		if len(shortName) > 16 {
			shortName = shortName[:16]
		}

		nodeName := locateFileNode(hrm, ringID, shortName)
		if nodeName == "" {
			nodeName = "UNKNOWN"
		}

		if _, err := fmt.Fprintf(writer, "%-46s\t%-6s\t%s\n", fileName, size, nodeName); err != nil {
			return fmt.Errorf("写入输出文件失败: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取输入文件失败: %w", err)
	}

	return nil
}

func loadDatasetFileNames(path string, limit int) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	names := make([]string, 0, limit)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) == 0 {
			continue
		}
		names = append(names, fields[0])
		if limit > 0 && len(names) >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func runRouteBenchmark(hrm *consistent.HashRingManager, path string, limit int) {
	names, err := loadDatasetFileNames(path, limit)
	if err != nil {
		log.Fatalf("读取路由数据失败: %v", err)
	}
	testRouteTime(hrm, names)
}

func benchmarkHashRingBuild(fabricClient *fabric.FabricClient, iterations int) {
	if iterations <= 0 {
		iterations = 1
	}

	outputFile, err := os.Create("hashring_build_benchmark.txt")
	if err != nil {
		log.Fatalf("创建哈希环构建基准测试日志文件失败: %v", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	fmt.Fprintf(writer, "序号\t状态\t耗时(ns)\t耗时(ms)\n")

	var total time.Duration
	success := 0

	for i := 0; i < iterations; i++ {
		start := time.Now()
		_, err := buildLocalHashRing(fabricClient)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Println("[基准] 第 %d 次构建哈希环失败: %v", i+1, err)
			fmt.Fprintf(writer, "%d\t失败\t%d\t%.3f\t%v\n",
				i+1, elapsed.Nanoseconds(), float64(elapsed.Nanoseconds())/1e6, err)
			continue
		}

		total += elapsed
		success++

		fmt.Fprintf(writer, "%d\t成功\t%d\t%.3f\n",
			i+1, elapsed.Nanoseconds(), float64(elapsed.Nanoseconds())/1e6)
	}

	fmt.Fprintf(writer, "\n=== 统计信息 ===\n")
	fmt.Fprintf(writer, "总次数: %d\n", iterations)
	fmt.Fprintf(writer, "成功次数: %d\n", success)
	fmt.Fprintf(writer, "失败次数: %d\n", iterations-success)
	fmt.Fprintf(writer, "总耗时: %v\n", total)

	if success == 0 {
		log.Println("[基准] 所有构建均失败，无法计算平均值")
		fmt.Fprintf(writer, "平均耗时: N/A (所有构建均失败)\n")
		return
	}

	avg := total / time.Duration(success)
	fmt.Fprintf(writer, "平均耗时: %v\n", avg)
	fmt.Fprintf(writer, "平均耗时(ns): %d\n", avg.Nanoseconds())
	fmt.Fprintf(writer, "平均耗时(ms): %.3f\n", float64(avg.Nanoseconds())/1e6)

	fmt.Printf("[基准] 成功构建 %d/%d 次，总耗时: %v, 平均耗时: %v\n", success, iterations, total, avg)
	fmt.Printf("[基准] 详细日志已保存到: hashring_build_benchmark.txt\n")
}

func main() {
	flag.IntVar(&usepipeline, "usepipeline", 0,
		"运行模式: 0=串行模式, 1=流水线模式, 2=处理统一数据映射, 3=路由时间测试, 4=哈希环构建基准测试")
	flag.IntVar(&workerCount, "workers", 4,
		"每个存储服务器连接的worker goroutine数量，用于并行处理响应")
	flag.Parse()

	fabricClient, err := fabric.InitFabricClientFromFlags()
	if err != nil {
		log.Fatalf("FabricClient 初始化失败: %v", err)
	}
	defer fabricClient.Close()

	if err := fabricClient.InitLedger(); err != nil {
		fmt.Println("账本初始化失败（可忽略已初始化错误）: %v", err)
	}

	displayRingStatus(fabricClient, "账本初始化后的初始状态")

	// addNodesToBlockchain(fabricClient)

	if err := fabricClient.InitLedgerWithCustomNodes(); err != nil {
		fmt.Println("账本添加节点失败: %v", err)
	}

	hrm, err := buildLocalHashRing(fabricClient)
	time.Sleep(5 * time.Second)
	if err != nil {
		log.Fatalf("构建本地哈希环失败: %v", err)
	}

	for ringID, ring := range hrm.GetAllRings() {
		fmt.Printf("Ring %s 上的节点: ", ringID)
		for _, node := range ring.GetMembers() {
			fmt.Printf("%s ", node.String())
		}
		fmt.Println()
	}
	time.Sleep(5 * time.Second)

	dir := "./cid_files"
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("读取目录失败: %v", err)
	}

	centerClient := network.NewTCPClient(centerServer)
	serverClients, err := prepareServerClients(centerClient)
	if err != nil {
		log.Fatalf("预连接服务器失败: %v", err)
	}
	defer closeServerClients(serverClients)

	switch usepipeline {
	case 0:
		start := time.Now()
		runSerialMode(hrm, centerClient, serverClients, files)
		//select {}
		fmt.Printf("[串行模式] 总耗时: %v\n", time.Since(start))

	case 1:
		start := time.Now()
		runPipelineMode(hrm, centerClient, serverClients, files)
		fmt.Printf("[流水线模式] 总耗时: %v\n", time.Since(start))

	case 2:
		fmt.Println("=== 模式 2: 处理统一数据映射 ===")
		if err := annotateUnifiedData(hrm,
			"/home/ipfs-dataset/50G_disk/whole_unified_ipfs_data.txt",
			"/home/ipfs-dataset/50G_disk/whole_unified_ipfs_data_with_node.txt"); err != nil {
			log.Fatalf("处理统一数据失败: %v", err)
		}
		fmt.Println("已生成 whole_unified_ipfs_data_with_node.txt")

	case 3:
		fmt.Println("=== 模式 3: 路由时间测试 ===")
		runRouteBenchmark(hrm, "/home/ipfs-dataset/50G_disk/whole_unified_ipfs_data.txt", 10851)

	case 4:
		fmt.Println("=== 模式 4: 哈希环构建基准测试 ===")
		benchmarkHashRingBuild(fabricClient, 10000)

	default:
		fmt.Println("未知的运行模式: %d", usepipeline)
	}
}
