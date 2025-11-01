package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hyperledger/client/pkg/fabric"
	"github.com/hyperledger/client/pkg/mode"
	"github.com/hyperledger/client/pkg/network"
)

/* const centerServer = "47.251.71.236:8081"

var ringToServer = map[int]string{
	0: "localhost:8081",
	1: "localhost:8080",
	2: "localhost:8082",
	3: "localhost:8083",
} */

const centerServer = "10.0.0.185:8082"

// var ringToServer = map[int]string{
// 	0: "47.251.95.138:8081",
// 	1: "47.251.95.138:8080",
// 	2: "47.251.95.138:8082",
// 	3: "47.251.95.138:8083",
// }

var ringToServer = map[int]string{
	0: "localhost:8082",
	1: "10.0.0.185:8082",
	4: "localhost:8082",
	5: "localhost:8082",
	2: "10.0.0.186:8082",
	3: "10.0.0.187:8082",
}

var usepipeline int
var workerCount int
var verifyProof bool
var configPath string

type PipelineConfig struct {
	CacheBytes     int64 `json:"cache_bytes"`
	FlushTimeoutMs int   `json:"flush_timeout_ms"`
}

func loadPipelineConfig(path string) PipelineConfig {
	// defaults
	cfg := PipelineConfig{
		CacheBytes:     16 * 1024 * 1024, // 16MB
		FlushTimeoutMs: 1000,
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg
	}
	var tmp PipelineConfig
	if err := json.Unmarshal(b, &tmp); err != nil {
		return cfg
	}
	if tmp.CacheBytes > 0 {
		cfg.CacheBytes = tmp.CacheBytes
	}
	if tmp.FlushTimeoutMs > 0 {
		cfg.FlushTimeoutMs = tmp.FlushTimeoutMs
	}
	return cfg
}

// main: 保留最小入口；helpers 与 run_modes 放在其它文件中
func main() {
	flag.IntVar(&usepipeline, "usepipeline", 0,
		"运行模式: 0=串行上传, 1=串行查询, 2=流水线查询, 3=路由时间测试, 4=哈希环构建基准测试")
	flag.IntVar(&workerCount, "workers", 4,
		"每个存储服务器连接的worker goroutine数量，用于并行处理响应")
	flag.BoolVar(&verifyProof, "verify", false, "是否在下载时校验 merkle proof（串行/流水线模式有效）")
	flag.StringVar(&configPath, "config", "config/pipeline.json", "流水线配置文件路径（JSON），包含 cache_bytes 与 flush_timeout_ms")
	flag.Parse()

	fabricClient, err := fabric.InitFabricClientFromFlags()
	if err != nil {
		log.Fatalf("FabricClient 初始化失败: %v", err)
	}
	defer fabricClient.Close()

	if usepipeline == 0 {
		if err := fabricClient.InitLedger(); err != nil {
			fmt.Printf("账本初始化失败（可忽略已初始化错误）: %v\n", err)
		}
		displayRingStatus(fabricClient, "账本初始化后的初始状态")
		if err := fabricClient.InitLedgerWithCustomNodes(); err != nil {
			fmt.Printf("账本添加节点失败: %v\n", err)
		}
	}

	// if err := fabricClient.InitLedger(); err != nil {
	// 	fmt.Printf("账本初始化失败（可忽略已初始化错误）: %v\n", err)
	// }

	displayRingStatus(fabricClient, "账本赋值后的状态")

	// if err := fabricClient.InitLedgerWithCustomNodes(); err != nil {
	// 	fmt.Printf("账本添加节点失败: %v\n", err)
	// }

	hrm, err := buildLocalHashRing(fabricClient)
	if err != nil {
		log.Fatalf("构建本地哈希环失败: %v", err)
	}
	time.Sleep(2 * time.Second)

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
		// 串行上传
		start := time.Now()
		mode.UploadSerial(hrm, centerClient, files)
		fmt.Printf("[串行上传] 总耗时: %v\n", time.Since(start))
	case 1:
		// 串行查询（下载）
		start := time.Now()
		mode.RunSerialMode(hrm, centerClient, serverClients, files, ringToServer, centerServer, verifyProof)
		fmt.Printf("[串行查询] 总耗时: %v\n", time.Since(start))
	case 2:
		// 流水线查询
		start := time.Now()
		cfg := loadPipelineConfig(configPath)
		mode.RunPipelineMode(hrm, serverClients, files, ringToServer, centerServer, workerCount, verifyProof, cfg.CacheBytes, time.Duration(cfg.FlushTimeoutMs)*time.Millisecond)
		fmt.Printf("[流水线查询] 总耗时: %v\n", time.Since(start))
	default:
		fmt.Printf("未知的运行模式: %d\n", usepipeline)
	}
}
