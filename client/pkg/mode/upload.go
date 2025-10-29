package mode

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hyperledger/client/pkg/network"
	"github.com/hyperledger/consistent"
)

// UploadSerial 逐个上传文件到中心服务器
func UploadSerial(hrm *consistent.HashRingManager, centerClient *network.TCPClient, files []os.DirEntry) {
	fmt.Println("=== 串行上传模式 ===")

	uploadCount := 0
	fmt.Println("开始串行上传文件...")
	for _, file := range files {
		if uploadCount >= 5000 || file.IsDir() {
			continue
		}

		filePath := filepath.Join("./cid_files", file.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Printf("读取文件失败: %s, err: %v\n", filePath, err)
			continue
		}

		hash := sha256.Sum256([]byte(file.Name()))
		fileHash := binary.BigEndian.Uint64(hash[:8])
		ringID := fmt.Sprintf("ring%d", fileHash%4)

		fileName := file.Name()
		if len(fileName) > 16 {
			fileName = fileName[:16]
		}
		nodeName := hrm.LocateKey(ringID, fileName)

		if nodeName == "" {
			fmt.Printf("未找到文件 %s 的归属节点，跳过\n", fileName)
			continue
		}

		if err := centerClient.UploadFile(nodeName, fileName, fileHash, data); err != nil {
			fmt.Printf("串行上传失败: %s, err: %v\n", fileName, err)
		} else {
			fmt.Printf("串行上传成功: %s -> %s (%s)\n", fileName, nodeName, ringID)
		}
		uploadCount++
	}
	fmt.Printf("串行上传完成，共上传 %d 个文件\n", uploadCount)
	select {}
}
