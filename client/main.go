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

func addNodesToBlockchain(fabricClient *fabric.FabricClient) {
	for i := 0; i < 40; i++ {
		serverName := fmt.Sprintf("node%d", i)
		ringID := fmt.Sprintf("ring%d", i%4)
		for {
			err := fabricClient.AddServerNode(ringID, serverName, "")
			if err != nil {
				log.Printf("添加节点失败: %s, err: %v", serverName, err)
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
						log.Printf("添加节点失败: %s, err: %v", serverName, err)
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
			log.Printf("关闭服务器连接失败(%s): %v", addr, err)
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

		filePath := filepath.Join("./files", file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("读取文件失败: %s, err: %v", filePath, err)
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
			log.Printf("未找到文件 %s 的归属节点，跳过", fileName)
			continue
		}

		if err := centerClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
			log.Printf("串行上传失败: %s, err: %v", fileName, err)
		} else {
			log.Printf("串行上传成功: %s -> %s (%s)", fileName, nodeName, ringID)
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
	// 		log.Printf("读取文件失败: %s, err: %v", filePath, err)
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
	// 		log.Printf("未找到文件 %s 的归属节点，跳过", fileName)
	// 		continue
	// 	}

	// 	// 根据环ID找到对应的服务器地址
	// 	ring := int(fileHash % 4)
	// 	serverAddr, ok := ringToServer[ring]
	// 	if !ok {
	// 		log.Printf("环%d没有对应的服务器地址，使用中心服务器: %s", ring, fileName)
	// 		serverAddr = centerServer
	// 	}

	// 	// 获取对应服务器的客户端连接
	// 	targetClient, exists := serverClients[serverAddr]
	// 	if !exists || targetClient == nil {
	// 		log.Printf("未找到服务器 %s 的连接，跳过: %s", serverAddr, fileName)
	// 		continue
	// 	}

	// 	// 直接上传到目标服务器
	// 	if err := targetClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
	// 		log.Printf("直接上传失败: %s -> %s (%s), err: %v", fileName, nodeName, serverAddr, err)

	// 		// 如果直接上传失败，回退到中心服务器
	// 		log.Printf("回退到中心服务器上传: %s", fileName)
	// 		if err := centerClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
	// 			log.Printf("中心服务器上传失败: %s, err: %v", fileName, err)
	// 		} else {
	// 			log.Printf("中心服务器上传成功: %s -> %s", fileName, nodeName)
	// 			uploadCount++
	// 		}
	// 	} else {
	// 		log.Printf("直接上传成功: %s -> %s (%s)", fileName, nodeName, serverAddr)
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
	// 		log.Printf("未找到文件 %s 的归属节点，跳过", fileName)
	// 		continue
	// 	}

	// 	serverAddr, ok := ringToServer[ring]
	// 	if !ok {
	// 		log.Printf("ring%d 没有对应的服务器地址，使用中心服务器: %s", ring, file.Name())
	// 		serverAddr = centerServer
	// 	}
	// 	queryClient, exists := serverClients[serverAddr]
	// 	if !exists || queryClient == nil {
	// 		log.Printf("未找到服务器 %s 的专属连接，跳过: %s", serverAddr, file.Name())
	// 		continue
	// 	}

	// 	returnedHash, data, err := queryClient.QueryFile(nodeName, fileHash)

	// 	if err != nil || len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
	// 		log.Printf("%d", data)
	// 		log.Printf("  客户端: %s", queryClient.Conn.RemoteAddr().String())
	// 		log.Printf("  服务器: %s", queryClient.Conn.LocalAddr().String())
	// 		log.Printf("从 %s 查询失败，回退到中心服务器: %s", serverAddr, file.Name())
	// 		returnedHash, data, err = centerClient.QueryFile(nodeName, fileHash)
	// 		if err != nil {
	// 			log.Printf("中心服务器查询失败: %s, err: %v", file.Name(), err)
	// 			continue
	// 		}
	// 	}

	// 	if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
	// 		log.Printf("服务端返回错误: %s, msg: %s", file.Name(), string(data))
	// 		continue
	// 	}

	// 	// 使用服务器返回的 hash 查找本地原始文件名（fallback 使用当前文件名）
	// 	outName, ok := hashToName[returnedHash]
	// 	if !ok {
	// 		outName = file.Name()
	// 	}

	// 	outPath := filepath.Join(receiveDir, outName)
	// 	if err := os.WriteFile(outPath, data, 0644); err != nil {
	// 		log.Printf("写入文件失败: %s, err: %v", outPath, err)
	// 	} else {
	// 		log.Printf("串行查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
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
			log.Printf(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
			continue
		}

		serverAddr, ok := ringToServer[ring]
		if !ok {
			logMsg := fmt.Sprintf("ring%d 没有对应的服务器地址，使用中心服务器: %s", ring, file.Name())
			log.Printf(logMsg)
			logger.Printf("WARN: %s", logMsg)
			serverAddr = centerServer
		}
		queryClient, exists := serverClients[serverAddr]
		if !exists || queryClient == nil {
			logMsg := fmt.Sprintf("未找到服务器 %s 的专属连接，跳过: %s", serverAddr, file.Name())
			log.Printf(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
			continue
		}

		returnedHash, data, err := queryClient.QueryFile(nodeName, fileHash)

		if err != nil || len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			log.Printf("%d", data)
			log.Printf("  客户端: %s", queryClient.Conn.RemoteAddr().String())
			log.Printf("  服务器: %s", queryClient.Conn.LocalAddr().String())
			logMsg := fmt.Sprintf("从 %s 查询失败，回退到中心服务器: %s", serverAddr, file.Name())
			log.Printf(logMsg)
			logger.Printf("FAIL: %s", logMsg)

			returnedHash, data, err = centerClient.QueryFile(nodeName, fileHash)
			if err != nil {
				logMsg := fmt.Sprintf("中心服务器查询失败: %s, err: %v", file.Name(), err)
				log.Printf(logMsg)
				logger.Printf("FAIL: %s", logMsg)
				failCount++
				continue
			}
		}

		if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			logMsg := fmt.Sprintf("服务端返回错误: %s, msg: %s", file.Name(), string(data))
			log.Printf(logMsg)
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
			log.Printf(logMsg)
			logger.Printf("FAIL: %s", logMsg)
			failCount++
		} else {
			logMsg := fmt.Sprintf("串行查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
			log.Printf(logMsg)
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

	queryPipeline := pipeline.NewClientPipeline(
		hashFunc,
		func(node string, tasks []*pipeline.FileTask) {
			ringGroups := make(map[int][]*pipeline.FileTask)
			for _, t := range tasks {
				if node == "" {
					log.Printf("[流水线] 未找到文件 %s 的归属节点，跳过", t.FileName)
					continue
				}
				ring := int(t.FileHash % 4)
				ringGroups[ring] = append(ringGroups[ring], t)
			}

			for ring, group := range ringGroups {
				if len(group) == 0 {
					continue
				}
				serverAddr, ok := ringToServer[ring]
				if !ok {
					log.Printf("[流水线] ring%d 没有对应的服务器地址，跳过该组", ring)
					continue
				}
				queryClient, exists := serverClients[serverAddr]
				if !exists || queryClient == nil {
					log.Printf("[流水线] 未找到服务器 %s 的专属连接，跳过该组", serverAddr)
					continue
				}

				batch := make([]struct {
					NodeName string
					FileHash uint64
				}, len(group))
				for i, t := range group {
					batch[i].NodeName = t.Node
					batch[i].FileHash = t.FileHash
				}

				hashes, results, errs := queryClient.BatchQueryFiles(batch)
				/*for i, t := range group {
					if errs[i] != nil {
						log.Printf("[流水线] 查询失败: %s, err: %v", t.FileName, errs[i])
						retHash, data, err := centerClient.QueryFile(t.Node, t.FileHash)
						if err != nil {
							log.Printf("[流水线] 中心服务器查询失败: %s, err: %v", t.FileName, err)
							continue
						}
						if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
							log.Printf("[流水线] 服务端返回错误: %s, msg: %s", t.FileName, string(data))
							continue
						}
						outName, ok := hashToName[retHash]
						if !ok {
							outName = t.FileName
						}
						if err := os.WriteFile(filepath.Join(receiveDir, outName), data, 0644); err != nil {
							log.Printf("[流水线] 写入文件失败: %s, err: %v", outName, err)
						} else {
							log.Printf("[流水线] 查询并写入成功(回退): %s (ring%d, server: %s)", outName, ring, centerServer)
						}
						continue
					}
					retHash := hashes[i]
					data := results[i]
					if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
						log.Printf("[流水线] 服务端返回错误: %s, msg: %s", t.FileName, string(data))
						continue
					}
					outName, ok := hashToName[retHash]
					if !ok {
						outName = t.FileName
					}
					if err := os.WriteFile(filepath.Join(receiveDir, outName), data, 0644); err != nil {
						log.Printf("[流水线] 写入文件失败: %s, err: %v", outName, err)
					} else {
						log.Printf("[流水线] 查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
					}
				}*/
				// 先处理目标服务器返回，收集需要回退到中心服务器的项
				var fallbackBatch []struct {
					NodeName string
					FileHash uint64
				}
				var fallbackIdxs []int // 保存原始 group 索引，便于回填结果

				for i, t := range group {
					// 出错或超出返回长度都视为需要回退
					if errs[i] != nil {
						log.Printf("[流水线] 目标服务器未返回或出错，回退到中心: %s", t.FileName)
						fallbackBatch = append(fallbackBatch, struct {
							NodeName string
							FileHash uint64
						}{NodeName: t.Node, FileHash: t.FileHash})
						fallbackIdxs = append(fallbackIdxs, i)
						continue
					}
					retHash := hashes[i]
					data := results[i]
					outName, ok := hashToName[retHash]
					if !ok {
						outName = t.FileName
					}
					if err := os.WriteFile(filepath.Join(receiveDir, outName), data, 0644); err != nil {
						log.Printf("[流水线] 写入文件失败: %s, err: %v", outName, err)
					} else {
						log.Printf("[流水线] 查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
					}
				}

				// 如果有需要回退的项，批量发送到中心服务器查询
				if len(fallbackBatch) > 0 {
					cHashes, cResults, cErrs := centerClient.BatchQueryFiles(fallbackBatch)
					for j, origIdx := range fallbackIdxs {
						t := group[origIdx]
						// 检查中心返回
						if j < len(cErrs) && cErrs[j] != nil {
							log.Printf("[流水线] 中心服务器回退查询失败: %s, err: %v", t.FileName, cErrs[j])
							continue
						}
						retHash := cHashes[j]
						data := cResults[j]
						outName, ok := hashToName[retHash]
						if !ok {
							outName = t.FileName
						}
						if err := os.WriteFile(filepath.Join(receiveDir, outName), data, 0644); err != nil {
							log.Printf("[流水线] 回退写入文件失败: %s, err: %v", outName, err)
						} else {
							log.Printf("[流水线] 查询并写入成功(回退): %s (ring%d, server: %s)", outName, ring, centerServer)
						}
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
		if queryCount >= 5000 || file.IsDir() {
			continue
		}
		queryPipeline.PushWorkload(file.Name(), nil)
		queryCount++
	}
	queryPipeline.Wait()
	fmt.Printf("查询流水线已完成，共查询 %d 个文件!\n", queryCount)
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
			log.Printf("[基准] 第 %d 次构建哈希环失败: %v", i+1, err)
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
	flag.Parse()

	fabricClient, err := fabric.InitFabricClientFromFlags()
	if err != nil {
		log.Fatalf("FabricClient 初始化失败: %v", err)
	}
	defer fabricClient.Close()

	if err := fabricClient.InitLedger(); err != nil {
		log.Printf("账本初始化失败（可忽略已初始化错误）: %v", err)
	}

	displayRingStatus(fabricClient, "账本初始化后的初始状态")

	// addNodesToBlockchain(fabricClient)

	if err := fabricClient.InitLedgerWithCustomNodes(); err != nil {
		log.Printf("账本添加节点失败: %v", err)
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

	dir := "./files"
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
		log.Printf("未知的运行模式: %d", usepipeline)
	}
}
