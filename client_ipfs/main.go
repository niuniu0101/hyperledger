package main

import (
	"context" // 引入 context
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net/http" // 引入 http
	"os"
	"path/filepath"
	"strings"
	"time"

	// "github.com/hyperledger/client/pkg/fabric"
	// "github.com/hyperledger/client/pkg/models" // 暂时不需要
	"github.com/hyperledger/client/pkg/network"
	"github.com/hyperledger/client/pkg/pipeline"

	// "github.com/hyperledger/consistent" // 移除一致性哈希库依赖

	// 引入 libp2p/DHT 依赖
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// --- 全局常量和配置 ---

const centerServer = "47.251.95.138:8082" // 中心服务器地址

// ringToServer: 保持原来的哈希环ID到服务器的映射，但现在我们直接使用公网IP和端口。
// 注意：这个映射在你的新模型中可能不再需要，因为查找服务器地址的任务交给了 DHT。
// 为了简化，我们暂时用它来映射 nodeName 到公网 IP，但这违背了 DHT 查找的目的。
// **更好的做法是在 DHT 查找成功后，直接使用查到的公网 IP。**
/* var ringToServer = map[int]string{
    0: "localhost:8082",
    1: "localhost:8082",
    4: "localhost:8082",
    5: "localhost:8082",
    2: "localhost:8082",
    3: "localhost:8082",
} */

var usepipeline int

// --- libp2p/DHT 相关结构和方法 ---

// DHTManager 封装 libp2p DHT 逻辑
type DHTManager struct {
	Host   host.Host
	DHT    *dht.IpfsDHT
	Config NodeConfig
	Server *http.Server
}

// NodeConfig 节点启动配置 (用于 DHTManager)
type NodeConfig struct {
	Role      string // 节点角色: "finder"
	Port      int
	HTTPPort  int
	Bootstrap string // 引导节点 Multiaddr 地址
	ListenIP  string // 监听 IP (通常是 0.0.0.0)
	PublicIP  string // 公网 IP (用于 Announce)
}

// createDHTManager 初始化 libp2p 节点 (仅作为 Finder 角色)
func createDHTManager(config NodeConfig) (*DHTManager, error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return nil, err
	}

	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", config.ListenIP, config.Port)

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	// finder 节点通常使用 ModeClient
	ipfsDHT, err := dht.New(
		context.Background(),
		host,
		dht.Mode(dht.ModeClient),
		dht.ProtocolPrefix("/ipfs"),
	)
	if err != nil {
		return nil, err
	}

	manager := &DHTManager{
		Host:   host,
		DHT:    ipfsDHT,
		Config: config,
	}

	if err := ipfsDHT.Bootstrap(context.Background()); err != nil {
		return nil, err
	}

	// 尝试连接到引导节点
	if config.Bootstrap != "" {
		go manager.connectToBootstrap(config.Bootstrap)
	}

	// 我们不需要完整的 HTTP API，但可以保留简单的状态检查

	log.Printf("🚀 Finder 节点启动成功! ID: %s", host.ID().ShortString())

	return manager, nil
}

func (m *DHTManager) connectToBootstrap(bootstrapAddr string) {
	time.Sleep(3 * time.Second)

	ma, err := multiaddr.NewMultiaddr(bootstrapAddr)
	if err != nil {
		log.Printf("❌ 解析引导节点地址失败: %v", err)
		return
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		log.Printf("❌ 解析引导节点信息失败: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("尝试连接到引导节点: %s...", addrInfo.ID.ShortString())
	if err := m.Host.Connect(ctx, *addrInfo); err != nil {
		log.Printf("❌ 连接到引导节点失败: %v", err)
	} else {
		log.Printf("✅ 成功连接到引导节点: %s", addrInfo.ID.ShortString())
	}
}

// findProviderPublicIP 查找指定 CID 对应的提供者公网 IP
func (m *DHTManager) findProviderPublicIP(c cid.Cid) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	providersChan := m.DHT.FindProvidersAsync(ctx, c, 1) // 只需要找到一个

	select {
	case provider := <-providersChan:
		if provider.ID == "" {
			return "", fmt.Errorf("找到的提供者 Peer ID 为空")
		}

		// 遍历提供者的所有地址，查找公网 IP
		for _, ma := range provider.Addrs {
			// 解析 multiaddr，寻找 /ip4/.../tcp/... 结构
			addrParts := strings.Split(ma.String(), "/")
			if len(addrParts) >= 3 && addrParts[1] == "ip4" {
				ip := addrParts[2]
				// 排除私有 IP 和回环地址
				if !isPrivateIP(ip) {
					// 假设公网 IP 后面紧跟的是 TCP 端口
					portPart := ""
					for i := 3; i < len(addrParts); i += 2 {
						if addrParts[i] == "tcp" && i+1 < len(addrParts) {
							portPart = addrParts[i+1]
							break
						}
					}

					if portPart != "" {
						log.Printf("🔍 DHT 找到提供者 [%s]，公网地址: %s:%s", provider.ID.ShortString(), ip, portPart)

						// **注意:** 这里我们只返回 IP，不带端口。
						// 因为后端服务器的通信端口 (8082) 可能与 libp2p 端口 (5002) 不同。
						return ip, nil
					}
				}
			}
		}
		return "", fmt.Errorf("找到提供者 Peer ID: %s，但未发现可用公网 IP", provider.ID.ShortString())

	case <-ctx.Done():
		return "", fmt.Errorf("DHT 查找超时或被取消: %w", ctx.Err())
	}
}

func parseCidString(cidStr string) (cid.Cid, error) {
	return cid.Decode(cidStr)
}

func isPrivateIP(ip string) bool {
	// 简化的私有 IP 检查：排除 127.x.x.x 和 10.x.x.x, 172.16-31.x.x, 192.168.x.x
	return strings.HasPrefix(ip, "127.") ||
		strings.HasPrefix(ip, "10.") ||
		strings.HasPrefix(ip, "192.168.") ||
		strings.HasPrefix(ip, "172.16.") ||
		strings.HasPrefix(ip, "172.17.") ||
		strings.HasPrefix(ip, "172.18.") ||
		strings.HasPrefix(ip, "172.19.") ||
		strings.HasPrefix(ip, "172.20.") ||
		strings.HasPrefix(ip, "172.21.") ||
		strings.HasPrefix(ip, "172.22.") ||
		strings.HasPrefix(ip, "172.23.") ||
		strings.HasPrefix(ip, "172.24.") ||
		strings.HasPrefix(ip, "172.25.") ||
		strings.HasPrefix(ip, "172.26.") ||
		strings.HasPrefix(ip, "172.27.") ||
		strings.HasPrefix(ip, "172.28.") ||
		strings.HasPrefix(ip, "172.29.") ||
		strings.HasPrefix(ip, "172.30.") ||
		strings.HasPrefix(ip, "172.31.")
}

// --- 统一的 DHT 查找客户端 (新) ---

// dhtClientCache 缓存基于公网 IP 地址的 TCP 连接
var dhtClientCache = make(map[string]*network.TCPClient)

// getClientForIP 获取或创建到指定 IP:8082 的 TCP 连接
func getClientForIP(ip string) (*network.TCPClient, error) {
	addr := fmt.Sprintf("%s:8082", ip) // 使用公网 IP + 固定的后端服务端口 8082

	if client, ok := dhtClientCache[addr]; ok {
		return client, nil
	}

	client := network.NewTCPClient(addr)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("连接到服务器 %s 失败: %w", addr, err)
	}

	dhtClientCache[addr] = client
	return client, nil
}

// closeAllDHTClients 关闭所有缓存的连接
func closeAllDHTClients() {
	for addr, client := range dhtClientCache {
		if err := client.Close(); err != nil {
			log.Printf("关闭连接失败(%s): %v", addr, err)
		}
	}
	dhtClientCache = make(map[string]*network.TCPClient) // 清空缓存
}

// --- 核心业务逻辑重写 (使用 DHT) ---

// 仅保留中心服务器连接，其他连接改为 DHT 动态获取
func prepareServerClients(centerClient *network.TCPClient) (map[string]*network.TCPClient, error) {
	// 仅连接中心服务器
	if err := centerClient.Connect(); err != nil {
		return nil, fmt.Errorf("中心服务器连接失败: %w", err)
	}

	// 返回一个只包含中心服务器的缓存，其他连接将在运行时动态创建和缓存到 dhtClientCache
	clientCache := make(map[string]*network.TCPClient, 1)
	clientCache[centerServer] = centerClient
	return clientCache, nil
}

func closeServerClients(cache map[string]*network.TCPClient) {
	// 关闭中心服务器连接
	if client, ok := cache[centerServer]; ok && client != nil {
		if err := client.Close(); err != nil {
			log.Printf("关闭中心服务器连接失败: %v", err)
		}
	}
	// 关闭 DHT 动态创建的连接
	closeAllDHTClients()
}

// runSerialMode 使用 DHT 查找公网 IP
func runSerialMode(dhtManager *DHTManager, centerClient *network.TCPClient, files []os.DirEntry) {
	fmt.Println("=== 使用串行模式 (DHT 路由) ===")

	// // --- 串行上传文件 (保留) ---
	// uploadCount := 0
	// fmt.Println("开始串行上传文件...")
	// for _, file := range files {
	// 	if uploadCount >= 1000 || file.IsDir() {
	// 		continue
	// 	}

	// 	filePath := filepath.Join("files", file.Name())
	// 	data, err := os.ReadFile(filePath)
	// 	if err != nil {
	// 		log.Printf("读取文件失败: %s, err: %v", filePath, err)
	// 		continue
	// 	}

	// 	hash := sha256.Sum256([]byte(file.Name()))
	// 	fileHash := binary.BigEndian.Uint64(hash[:8])
	// 	// ringID := fmt.Sprintf("ring%d", fileHash%4) // 不再用于路由，但保留用于数据结构

	// 	// **注意:** 在没有哈希环的情况下，我们无法确定 nodeName。
	// 	// 为了满足 UploadFile 的接口，我们**假设**所有文件都由一个默认节点处理，
	// 	// 或者你必须通过其他方式（例如 Fabric）获取目标 nodeName。
	// 	// 这里，我们假设目标 nodeName 必须是已知的，我们使用一个占位符。
	// 	nodeName := "node0" // ⚠️ 占位符：如果后端需要真实的 nodeName，你需要一个映射或查找机制

	// 	// 由于 DHT 查找的是 CID，因此这里需要一个机制将文件名转换为 CID。
	// 	// 由于你的代码中没有实际的 IPFS/CID 生成逻辑，这里先假设文件名就是 CID 对应的文件。
	// 	// **!!! 关键假设: 文件名本身是一个有效的 CID 字符串 !!!**
	// 	cidStr := file.Name()
	// 	c, err := parseCidString(cidStr)
	// 	if err != nil {
	// 		// 如果文件名不是有效的 CID，则跳过上传
	// 		log.Printf("文件名 %s 不是有效的 CID，跳过上传", cidStr)
	// 		continue
	// 	}

	// 	// 查找提供者公网 IP
	// 	providerIP, err := dhtManager.findProviderPublicIP(c)
	// 	if err != nil {
	// 		log.Printf("上传前 DHT 查找失败: %s, err: %v", cidStr, err)
	// 		// 上传时找不到公网 IP 是正常的，因为可能是第一次上传，所以仍然走中心服务器
	// 		// 正常情况下，上传应该直接走中心服务器或目标服务器
	// 		// 这里的目的是为了模拟，所以我们跳过查找，直接使用中心服务器连接

	// 		if err := centerClient.UploadFile(nodeName, cidStr, fileHash, data); err != nil {
	// 			log.Printf("串行上传失败: %s, err: %v", cidStr, err)
	// 		} else {
	// 			log.Printf("串行上传成功: %s -> %s (中心服务器)", cidStr, nodeName)
	// 		}
	// 	} else {
	// 		// 如果查找到了公网 IP，我们假设直接上传到该服务器
	// 		targetClient, err := getClientForIP(providerIP)
	// 		if err != nil {
	// 			log.Printf("上传时连接到目标服务器 %s 失败: %v", providerIP, err)
	// 			if err := centerClient.UploadFile(nodeName, cidStr, fileHash, data); err != nil {
	// 				log.Printf("串行上传失败: %s, err: %v", cidStr, err)
	// 			} else {
	// 				log.Printf("串行上传成功: %s -> %s (回退到中心服务器)", cidStr, nodeName)
	// 			}
	// 		} else {
	// 			if err := targetClient.UploadFile(nodeName, cidStr, fileHash, data); err != nil {
	// 				log.Printf("串行上传失败: %s, err: %v", cidStr, err)
	// 			} else {
	// 				log.Printf("串行上传成功: %s -> %s (公网IP: %s)", cidStr, nodeName, providerIP)
	// 			}
	// 		}
	// 	}

	// 	uploadCount++
	// }
	fmt.Printf("串行上传完成")

	// --- 串行查询文件 ---

	receiveDir := "file_receive_dht"
	if err := os.MkdirAll(receiveDir, 0755); err != nil {
		log.Fatalf("创建接收目录失败: %v", err)
	}

	// 构造本地的 fileHash -> 原始文件名 映射
	hashToName := make(map[uint64]string, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		// name := "QmSnXZWZaJDzjnq2gXDbsK2yJd27FHjhmmgvfdTdxRtxLy"
		hash := sha256.Sum256([]byte(name))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		// name := "QmSnXZWZaJDzjnq2"
		// fileHash := uint64(11909429820657505293)
		hashToName[fileHash] = name
	}

	queryCount := 0
	fmt.Println("开始串行查询文件 (DHT 路由)...")

	for _, file := range files {
		if queryCount >= 1000 || file.IsDir() {
			continue
		}

		cidStr := file.Name() // 假设文件名是 CID

		c, err := parseCidString(cidStr)
		if err != nil {
			log.Printf("文件名 %s 不是有效的 CID，跳过查询", cidStr)
			continue
		}

		hash := sha256.Sum256([]byte(cidStr))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		nodeName := "node1" // ⚠️ 占位符

		// 1. **DHT 路由查询**：获取公网 IP
		providerIP, err := dhtManager.findProviderPublicIP(c)
		if err != nil {
			// DHT 查找失败，回退到中心服务器进行查询
			log.Printf("DHT 查找失败，回退到中心服务器查询: %s, err: %v", cidStr, err)
			providerIP = "" // 标记为回退
		}

		// 2. **通信**：根据结果决定使用哪个客户端
		var queryClient *network.TCPClient
		serverAddr := centerServer

		if providerIP != "" {
			// 使用 DHT 查到的公网 IP
			queryClient, err = getClientForIP(providerIP)
			if err != nil {
				log.Printf("连接到 DHT 目标服务器 %s 失败，回退到中心服务器", providerIP)
				queryClient = centerClient
				serverAddr = centerServer
			} else {
				serverAddr = fmt.Sprintf("%s:8082", providerIP)
			}
		} else {
			// 使用中心服务器
			queryClient = centerClient
			serverAddr = centerServer
		}

		// 3. **执行查询**
		returnedHash, data, err := queryClient.QueryFile(nodeName, fileHash)

		log.Printf("ip和文件哈希: %s, %d", serverAddr, fileHash)

		// 打印结果
		log.Printf("查询结果: %s, %s, %v", returnedHash, data, err)

		if err != nil || len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			// 如果在 DHT 目标或中心服务器上查询失败，我们已经尽力了
			log.Printf("从 %s 查询文件 %s 最终失败, err: %v", serverAddr, file.Name(), err)
			continue
		}

		if len(data) < 8 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			log.Printf("服务端返回错误: %s, msg: %s", file.Name(), string(data))
			continue
		}

		outName, ok := hashToName[returnedHash]
		if !ok {
			outName = file.Name()
		}

		outPath := filepath.Join(receiveDir, outName)
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			log.Printf("写入文件失败: %s, err: %v", outPath, err)
		} else {
			log.Printf("串行查询并写入成功: %s (server: %s)", outName, serverAddr)
		}
		queryCount++
	}

	fmt.Printf("串行查询完成，共查询 %d 个文件\n", queryCount)
}

// runPipelineMode (保留流水线逻辑，但移除哈希环依赖)
func runPipelineMode(dhtManager *DHTManager, centerClient *network.TCPClient, files []os.DirEntry) {
	fmt.Println("=== 使用流水线模式 (DHT 路由) ===")
	log.Printf("⚠️ 流水线模式使用 DHT 查找公网 IP 存在性能风险，此处仅保留结构")

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

	// HashFunc 负责路由，现在是 DHT 查找
	// 它返回 (nodeName, fileHash)
	hashFunc := func(fileName string) (string, uint64) {
		c, err := parseCidString(fileName)
		if err != nil {
			return "", 0 // 文件名不是 CID，无法路由
		}

		// 1. DHT 查找公网 IP
		providerIP, err := dhtManager.findProviderPublicIP(c)
		if err != nil {
			log.Printf("[流水线] DHT 查找失败: %s, err: %v", fileName, err)
			return "", 0
		}

		hash := sha256.Sum256([]byte(fileName))
		fileHash := binary.BigEndian.Uint64(hash[:8])

		// 返回公网 IP 作为 "node"
		return providerIP, fileHash
	}

	receiveDir := "file_receive_dht"
	if err := os.MkdirAll(receiveDir, 0755); err != nil {
		log.Fatalf("创建接收目录失败: %v", err)
	}

	// SendFunc 负责通信
	queryPipeline := pipeline.NewClientPipeline(
		hashFunc,
		func(providerIP string, tasks []*pipeline.FileTask) {
			if providerIP == "" {
				log.Printf("[流水线] 节点公网 IP 为空，跳过该批次")
				return
			}

			// 获取目标服务器连接
			queryClient, err := getClientForIP(providerIP)
			if err != nil {
				log.Printf("[流水线] 连接到目标服务器 %s:8082 失败，回退到中心服务器", providerIP)
				// 如果连接目标服务器失败，使用中心服务器处理这批任务
				queryClient = centerClient
				providerIP = "中心服务器"
			}

			batch := make([]struct {
				NodeName string
				FileHash uint64
			}, len(tasks))
			for i, t := range tasks {
				// ⚠️ 占位符
				batch[i].NodeName = "node0"
				batch[i].FileHash = t.FileHash
			}

			hashes, results, errs := queryClient.BatchQueryFiles(batch)

			// 处理结果
			for i, t := range tasks {
				// ... (简化结果处理，此处仅处理成功情况) ...
				if i >= len(results) || errs[i] != nil || len(results[i]) < 8 || (len(results[i]) >= 5 && string(results[i][:5]) == "ERROR") {
					log.Printf("[流水线] 查询失败或错误返回: %s (服务器: %s)", t.FileName, providerIP)
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
					log.Printf("[流水线] 查询并写入成功: %s (服务器: %s)", outName, providerIP)
				}
			}
		},
		15,
	)
	queryPipeline.SetFlushTimeout(1 * time.Second)

	queryCount := 0
	fmt.Println("开始流水线查询文件 (DHT 路由)...")
	for _, file := range files {
		if queryCount >= 1000 || file.IsDir() {
			continue
		}
		queryPipeline.PushWorkload(file.Name(), nil)
		queryCount++
	}
	queryPipeline.Wait()
	fmt.Printf("查询流水线已完成，共查询 %d 个文件!\n", queryCount)
}

func main() {
	// --- 命令行参数 (新增 DHT 参数) ---
	var dhtConfig NodeConfig

	flag.IntVar(&usepipeline, "usepipeline", 0,
		"运行模式: 0=串行模式, 1=流水线模式 (注意：模式 2, 3, 4 已移除)")

	// DHT 相关的 flag
	flag.IntVar(&dhtConfig.Port, "dht-port", 5003, "DHT节点端口")
	flag.StringVar(&dhtConfig.Bootstrap, "dht-bootstrap", "", "引导节点 Multiaddr 地址")
	flag.StringVar(&dhtConfig.ListenIP, "dht-ip", "0.0.0.0", "DHT 监听 IP 地址")
	flag.StringVar(&dhtConfig.PublicIP, "dht-public-ip", "", "DHT 服务器公网 IP (用于 Announce)")

	flag.Parse()

	// 设置 DHT 角色为 Finder
	dhtConfig.Role = "finder"

	// 初始化 DHT 客户端
	dhtManager, err := createDHTManager(dhtConfig)
	if err != nil {
		log.Fatalf("❌ 创建 DHT Manager 失败: %v", err)
	}

	dir := "/root/go/src/github.com/hyperledger/client_ipfs/cid_files"
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("读取目录失败: %v", err)
	}

	// 初始化中心服务器连接，并注册关闭
	centerClient := network.NewTCPClient(centerServer)
	serverClients, err := prepareServerClients(centerClient)
	if err != nil {
		log.Fatalf("预连接服务器失败: %v", err)
	}
	defer closeServerClients(serverClients)
	defer closeAllDHTClients() // 确保关闭所有动态创建的连接

	// --- 运行模式 ---

	switch usepipeline {
	case 0:
		start := time.Now()
		runSerialMode(dhtManager, centerClient, files)
		fmt.Printf("[串行模式] 总耗时: %v\n", time.Since(start))

	case 1:
		start := time.Now()
		runPipelineMode(dhtManager, centerClient, files)
		fmt.Printf("[流水线模式] 总耗时: %v\n", time.Since(start))

	default:
		log.Printf("未知的运行模式: %d (模式 2, 3, 4 已移除)", usepipeline)
	}
}
