package mode

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/hyperledger/client/pkg/network"
	"github.com/hyperledger/consistent"
)

// HRMRootGetter adapts HashRingManager to provide GetServerMerkleRoot(name) (string, error)
type HRMRootGetter struct {
	HRM *consistent.HashRingManager
}

func (h *HRMRootGetter) GetServerMerkleRoot(name string) (string, error) {
	if h == nil || h.HRM == nil {
		return "", fmt.Errorf("hrm not available")
	}
	if v, ok := h.HRM.GetMerkleRootHash(name); ok {
		// strip 0x prefix if present
		if len(v) >= 2 && (v[0:2] == "0x" || v[0:2] == "0X") {
			v = v[2:]
		}
		return v, nil
	}
	return "", fmt.Errorf("no merkle root for node %s", name)
}

// RunSerialMode 执行串行查询模式
func RunSerialMode(hrm *consistent.HashRingManager, centerClient *network.TCPClient, serverClients map[string]*network.TCPClient, files []os.DirEntry, ringToServer map[int]string, centerServer string, verify bool) {
	fmt.Println("=== 使用串行下载模式 ===")

	receiveDir := "file_receive"
	if err := os.MkdirAll(receiveDir, 0755); err != nil {
		log.Fatalf("创建接收目录失败: %v", err)
	}

	hashToName := make(map[uint64]string, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		h := sha256.Sum256([]byte(name))
		fh := binary.BigEndian.Uint64(h[:8])
		hashToName[fh] = name
	}

	queryCount := 0
	successCount := 0
	failCount := 0
	fmt.Println("开始串行查询文件...")

	// prepare adapter if verification is requested
	var adapter interface{}
	if verify {
		adapter = &HRMRootGetter{HRM: hrm}
	}

	for _, file := range files {
		if queryCount >= 30 || file.IsDir() {
			continue
		}

		hash := sha256.Sum256([]byte(file.Name()))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		ring := int(fileHash % 4)
		ringID := fmt.Sprintf("ring%d", ring)

		fileName := file.Name()
		if len(fileName) > 16 {
			fileName = fileName[:16]
		}
		nodeName := hrm.LocateKey(ringID, fileName)

		if nodeName == "" {
			logMsg := fmt.Sprintf("未找到文件 %s 的归属节点，跳过", fileName)
			fmt.Println(logMsg)
			failCount++
			queryCount++
			continue
		}

		serverAddr, ok := ringToServer[ring]
		if !ok {
			serverAddr = centerServer
		}
		queryClient, exists := serverClients[serverAddr]
		if !exists || queryClient == nil {
			logMsg := fmt.Sprintf("未找到服务器 %s 的专属连接，跳过: %s", serverAddr, file.Name())
			fmt.Println(logMsg)
			failCount++
			queryCount++
			continue
		}

		returnedHash, data, err := queryClient.QueryFile(nodeName, fileHash, adapter, verify)
		if err != nil || len(data) < 1 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			if err != nil {
				fmt.Printf("从 %s 查询失败: %v，回退中心\n", serverAddr, err)
			}
			returnedHash, data, err = centerClient.QueryFile(nodeName, fileHash, adapter, verify)
			if err != nil {
				logMsg := fmt.Sprintf("中心服务器查询失败: %s, err: %v", file.Name(), err)
				fmt.Println(logMsg)
				failCount++
				queryCount++
				continue
			}
		}

		if len(data) == 0 || (len(data) >= 5 && string(data[:5]) == "ERROR") {
			logMsg := fmt.Sprintf("服务端返回错误: %s, msg: %s", file.Name(), string(data))
			fmt.Println(logMsg)
			failCount++
			queryCount++
			continue
		}

		outName := file.Name()
		if out, ok := hashToName[returnedHash]; ok {
			outName = out
		}
		outPath := filepath.Join(receiveDir, outName)
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			logMsg := fmt.Sprintf("写入文件失败: %s, err: %v", outPath, err)
			fmt.Println(logMsg)
			failCount++
		} else {
			logMsg := fmt.Sprintf("串行查询并写入成功: %s (ring%d, server: %s)", outName, ring, serverAddr)
			fmt.Println(logMsg)
			successCount++
		}
		queryCount++
	}

	summary := fmt.Sprintf("串行查询完成，共查询 %d 个文件，成功 %d 个，失败 %d 个", queryCount, successCount, failCount)
	fmt.Println(summary)
}
