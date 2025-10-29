package consistent

import (
	"log"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// Node 表示一个可以加入哈希环的节点
type Node interface {
	GetName() string
}

// ServerNode 基础服务器节点结构
type ServerNode struct {
	ServerName     string `json:"serverName"`
	MerkleRootHash string `json:"merkleRootHash"`
}

// GetName 实现Node接口
func (s *ServerNode) GetName() string {
	return s.ServerName
}

// AllRingsContainer 包含多个环的容器
type AllRingsContainer struct {
	Rings map[string][]ServerNode `json:"rings"`
}

// ConsistentNode 是一个适配器，用于让任何节点实现 consistent.Member 接口
type ConsistentNode struct {
	Name string
}

func (m ConsistentNode) String() string {
	return m.Name
}

// CustomXXHasher 适配器，实现 consistent.Hasher 接口
type CustomXXHasher struct{}

func (h CustomXXHasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// HashRingManager 管理多个一致性哈希环
type HashRingManager struct {
	rings map[string]*consistent.Consistent
	// serverRoots 缓存每个 serverName 对应的 Merkle 根（从链码返回的结构中提取）
	serverRoots map[string]string
}

// NewHashRingManager 创建新的哈希环管理器
func NewHashRingManager() *HashRingManager {
	return &HashRingManager{
		rings:       make(map[string]*consistent.Consistent),
		serverRoots: make(map[string]string),
	}
}

// BuildFromNodes 从节点列表构建单个哈希环
func (hrm *HashRingManager) BuildFromNodes(ringID string, nodes []Node) {
	if len(nodes) == 0 {
		log.Printf("  - 环 '%s' 为空，跳过构建。", ringID)
		return
	}

	var consistentNodes []consistent.Member
	for _, node := range nodes {
		consistentNodes = append(consistentNodes, ConsistentNode{Name: node.GetName()})
	}

	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            CustomXXHasher{},
	}

	consistentHash := consistent.New(consistentNodes, cfg)
	hrm.rings[ringID] = consistentHash
	log.Printf("  - 已为环 '%s' 成功构建哈希实例，成员: %v", ringID, consistentHash.GetMembers())
}

// BuildHashRingsFromData 从容器数据构建多个本地一致性哈希环
func (hrm *HashRingManager) BuildHashRingsFromData(container *AllRingsContainer) {
	log.Println("\n--> 正在为每个环构建本地一致性哈希实例...")

	// 清空现有环
	hrm.rings = make(map[string]*consistent.Consistent)
	// 清空并准备缓存 server->merkleRoot 映射
	hrm.serverRoots = make(map[string]string)

	for ringID, nodes := range container.Rings {
		// 将ServerNode转换为Node接口
		nodeList := make([]Node, len(nodes))
		for i, node := range nodes {
			// ****** 修复后的代码：创建 node 的副本，并取副本的地址 ******
			// 否则所有元素都指向循环变量 `node` 的同一个内存地址
			newNode := node
			nodeList[i] = &newNode
			// ********************************************************
			// 同时将节点的 MerkleRootHash 缓存到 manager 中（如果有）
			if newNode.ServerName != "" {
				hrm.serverRoots[newNode.ServerName] = newNode.MerkleRootHash
			}
		}
		hrm.BuildFromNodes(ringID, nodeList)
	}
}

// GetMerkleRootHash 返回指定 serverName 的 Merkle 根（链码中登记的值），
// 如果存在返回 (hash, true)，否则返回 ("", false)
func (hrm *HashRingManager) GetMerkleRootHash(serverName string) (string, bool) {
	if hrm == nil || hrm.serverRoots == nil {
		return "", false
	}
	v, ok := hrm.serverRoots[serverName]
	return v, ok
}

// GetRing 获取指定ID的哈希环
func (hrm *HashRingManager) GetRing(ringID string) *consistent.Consistent {
	return hrm.rings[ringID]
}

// GetAllRings 获取所有哈希环
func (hrm *HashRingManager) GetAllRings() map[string]*consistent.Consistent {
	return hrm.rings
}

// LocateKey 在指定环中定位键的归属节点
func (hrm *HashRingManager) LocateKey(ringID, key string) string {
	if ring, exists := hrm.rings[ringID]; exists {
		member := ring.LocateKey([]byte(key))
		if member != nil {
			return member.String()
		}
	}
	return ""
}

// AddNode 向指定环添加节点
func (hrm *HashRingManager) AddNode(ringID string, node Node) {
	if ring, exists := hrm.rings[ringID]; exists {
		ring.Add(ConsistentNode{Name: node.GetName()})
	}
}

// RemoveNode 从指定环移除节点
func (hrm *HashRingManager) RemoveNode(ringID string, nodeName string) {
	if ring, exists := hrm.rings[ringID]; exists {
		ring.Remove(nodeName)
	}
}

// GetMembers 获取指定环的所有成员
func (hrm *HashRingManager) GetMembers(ringID string) []consistent.Member {
	if ring, exists := hrm.rings[ringID]; exists {
		return ring.GetMembers()
	}
	return nil
}
