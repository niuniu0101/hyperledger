package merkleroot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// CalculateNodeMerkleRoot 计算指定节点目录下所有块文件的默克尔根哈希
// nodeName: 节点名称，如 "node1", "node2"
// baseDir: 基础目录"server"
func CalculateNodeMerkleRoot(nodeName, baseDir string) (string, error) {
	nodeDir := filepath.Join(baseDir, nodeName)

	// 检查节点目录是否存在
	if _, err := os.Stat(nodeDir); os.IsNotExist(err) {
		return "", fmt.Errorf("node directory %s does not exist", nodeDir)
	}

	// 读取目录下的所有块文件（只包含chunk文件）
	files, err := getChunkFilesInDirectory(nodeDir)
	if err != nil {
		return "", fmt.Errorf("failed to read directory %s: %v", nodeDir, err)
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no chunk files found in directory %s", nodeDir)
	}

	// 计算所有块文件的哈希值
	fileHashes, err := calculateFileHashes(files)
	if err != nil {
		return "", fmt.Errorf("failed to calculate file hashes: %v", err)
	}

	// 构建默克尔树并返回根哈希
	merkleRoot := buildMerkleTree(fileHashes)
	return hex.EncodeToString(merkleRoot), nil
}

// CalculateAllNodesMerkleRoots 计算server目录下所有nodeN目录的默克尔根哈希
func CalculateAllNodesMerkleRoots(baseDir string) (map[string]string, error) {
	results := make(map[string]string)

	// 读取baseDir下的所有子目录
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read base directory: %v", err)
	}

	// 查找所有nodeN目录
	for _, entry := range entries {
		if entry.IsDir() {
			dirName := entry.Name()
			// 检查是否是nodeN格式的目录
			if isNodeDirectory(dirName) {
				merkleRoot, err := CalculateNodeMerkleRoot(dirName, baseDir)
				if err != nil {
					// 如果目录为空，跳过而不是报错
					if err.Error() == fmt.Sprintf("no chunk files found in directory %s", filepath.Join(baseDir, dirName)) {
						fmt.Printf("Warning: No chunk files in %s, skipping\n", dirName)
						continue
					}
					return nil, fmt.Errorf("failed to calculate merkle root for %s: %v", dirName, err)
				}
				results[dirName] = merkleRoot
				// fmt.Printf("Calculated merkle root for %s: %s\n", dirName, merkleRoot)
			}
		}
	}

	return results, nil
}

// getChunkFilesInDirectory 获取目录下的所有块文件（以chunk_开头，.dat结尾）
func getChunkFilesInDirectory(dirPath string) ([]string, error) {
	var files []string

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			fileName := entry.Name()
			// 只选择chunk文件
			if isChunkFile(fileName) {
				files = append(files, filepath.Join(dirPath, fileName))
			}
		}
	}

	// 按文件名排序以确保一致性
	sort.Strings(files)
	return files, nil
}

// isChunkFile 检查文件是否是块文件
func isChunkFile(fileName string) bool {
	// 块文件命名格式：chunk_数字.dat
	if len(fileName) < 11 { // "chunk_1.dat" 最小长度
		return false
	}

	// 检查前缀和后缀
	if fileName[:6] == "chunk_" && filepath.Ext(fileName) == ".dat" {
		// 检查中间部分是否是数字
		middlePart := fileName[6 : len(fileName)-4]
		for _, char := range middlePart {
			if char < '0' || char > '9' {
				return false
			}
		}
		return true
	}
	return false
}

// calculateFileHashes 计算所有文件的SHA256哈希
func calculateFileHashes(filePaths []string) ([][]byte, error) {
	var hashes [][]byte

	for _, filePath := range filePaths {
		hash, err := calculateFileHash(filePath)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}

	return hashes, nil
}

// calculateFileHash 计算单个文件的SHA256哈希
func calculateFileHash(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}

// buildMerkleTree 从文件哈希列表构建默克尔树，返回根哈希
func buildMerkleTree(hashes [][]byte) []byte {
	if len(hashes) == 0 {
		return nil
	}

	if len(hashes) == 1 {
		return hashes[0]
	}

	var newLevel [][]byte

	// 处理成对的节点
	for i := 0; i < len(hashes); i += 2 {
		if i+1 < len(hashes) {
			// 有左右两个节点
			newHash := calculateNodeHash(hashes[i], hashes[i+1])
			newLevel = append(newLevel, newHash)
		} else {
			// 奇数个节点时，复制最后一个节点
			newHash := calculateNodeHash(hashes[i], hashes[i])
			newLevel = append(newLevel, newHash)
		}
	}

	// 递归构建
	return buildMerkleTree(newLevel)
}

// calculateNodeHash 计算两个子节点哈希的父节点哈希
func calculateNodeHash(leftHash, rightHash []byte) []byte {
	hash := sha256.New()
	hash.Write(leftHash)
	hash.Write(rightHash)
	return hash.Sum(nil)
}

// isNodeDirectory 检查目录名是否符合nodeN格式
func isNodeDirectory(dirName string) bool {
	if len(dirName) < 5 {
		return false
	}

	// 检查是否以"node"开头
	if dirName[:4] == "node" {
		// 检查剩余部分是否都是数字
		for _, char := range dirName[4:] {
			if char < '0' || char > '9' {
				return false
			}
		}
		return true
	}
	return false
}
