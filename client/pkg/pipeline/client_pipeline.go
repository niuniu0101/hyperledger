package pipeline

import (
	"time"
)

// FileTask 表示一个待处理的文件任务
type FileTask struct {
	FileName string
	Data     []byte
	Size     int
	Node     string
	FileHash uint64
}

// HashFunc 计算节点名和哈希的函数签名
type HashFunc func(fileName string) (string, uint64)

// NodeCacheConfig 节点缓存配置
type NodeCacheConfig struct {
	DefaultCacheSize int
	nodeCacheSizes   map[string]int
	// 以字节为单位的缓存上限；若>0，则优先按字节决定是否flush
	DefaultCacheBytes int64
	nodeCacheBytes    map[string]int64
}

func NewNodeCacheConfig(defaultSize int) *NodeCacheConfig {
	return &NodeCacheConfig{
		DefaultCacheSize:  defaultSize,
		nodeCacheSizes:    make(map[string]int),
		DefaultCacheBytes: 0,
		nodeCacheBytes:    make(map[string]int64),
	}
}

func (c *NodeCacheConfig) SetNodeCacheSize(node string, size int) {
	c.nodeCacheSizes[node] = size
}

func (c *NodeCacheConfig) GetNodeCacheSize(node string) int {
	if size, ok := c.nodeCacheSizes[node]; ok {
		return size
	}
	return c.DefaultCacheSize
}

// SetNodeCacheBytes 设置某个节点的缓存容量（字节）
func (c *NodeCacheConfig) SetNodeCacheBytes(node string, sizeBytes int64) {
	c.nodeCacheBytes[node] = sizeBytes
}

// GetNodeCacheBytes 获取某个节点的缓存容量（字节）；如果未设置，返回全局默认
func (c *NodeCacheConfig) GetNodeCacheBytes(node string) int64 {
	if v, ok := c.nodeCacheBytes[node]; ok {
		return v
	}
	return c.DefaultCacheBytes
}

// ClientPipeline 客户端流水线结构（串行版本）
type ClientPipeline struct {
	cacheMap       map[string][]*FileTask
	cacheConfig    *NodeCacheConfig
	cacheTimestamp map[string]time.Time
	cacheBytes     map[string]int64
	sendFunc       func(node string, tasks []*FileTask)
	hashFunc       HashFunc
	flushTimeout   time.Duration
	tasks          []*FileTask
	// 可选：用于在处理阶段填充 FileTask.Size（字节），便于按字节缓存
	sizeFunc func(fileName string, fileHash uint64) int
}

// NewClientPipeline 创建流水线
func NewClientPipeline(
	hashFunc HashFunc,
	sendFunc func(node string, tasks []*FileTask),
	defaultCacheSize int,
) *ClientPipeline {
	return &ClientPipeline{
		cacheMap:       make(map[string][]*FileTask),
		cacheConfig:    NewNodeCacheConfig(defaultCacheSize),
		cacheTimestamp: make(map[string]time.Time),
		cacheBytes:     make(map[string]int64),
		sendFunc:       sendFunc,
		hashFunc:       hashFunc,
		flushTimeout:   100 * time.Millisecond,
		tasks:          make([]*FileTask, 0),
	}
}

func (p *ClientPipeline) SetFlushTimeout(timeout time.Duration) {
	p.flushTimeout = timeout
}

func (p *ClientPipeline) SetNodeCacheSize(node string, size int) {
	p.cacheConfig.SetNodeCacheSize(node, size)
}

// SetDefaultCacheBytes 设置全局按字节缓存上限（优先于按数量）
func (p *ClientPipeline) SetDefaultCacheBytes(sizeBytes int64) {
	p.cacheConfig.DefaultCacheBytes = sizeBytes
}

// SetNodeCacheBytes 设置某个节点的按字节缓存上限
func (p *ClientPipeline) SetNodeCacheBytes(node string, sizeBytes int64) {
	p.cacheConfig.SetNodeCacheBytes(node, sizeBytes)
}

// SetSizeFunc 设置大小解析函数（用于为每个任务提供 size hint）
func (p *ClientPipeline) SetSizeFunc(f func(fileName string, fileHash uint64) int) {
	p.sizeFunc = f
}

func (p *ClientPipeline) PushWorkload(fileName string, data []byte) {
	task := &FileTask{
		FileName: fileName,
		Data:     data,
		Size:     len(data),
	}
	p.tasks = append(p.tasks, task)
}

func (p *ClientPipeline) shouldFlush(node string) bool {
	// 仅按字节阈值 + 时间窗口 判断
	capBytes := p.cacheConfig.GetNodeCacheBytes(node)
	if capBytes > 0 && p.cacheBytes[node] >= capBytes {
		return true
	}
	if timestamp, exists := p.cacheTimestamp[node]; exists {
		if time.Since(timestamp) >= p.flushTimeout {
			return true
		}
	}
	return false
}

func (p *ClientPipeline) processTask(task *FileTask) {
	node, fileHash := p.hashFunc(task.FileName)
	task.Node = node
	task.FileHash = fileHash
	if p.sizeFunc != nil {
		if sz := p.sizeFunc(task.FileName, fileHash); sz > 0 {
			task.Size = sz
		}
	}

	cache := p.cacheMap[task.Node]
	if len(cache) == 0 {
		p.cacheTimestamp[task.Node] = time.Now()
	}
	cache = append(cache, task)
	p.cacheMap[task.Node] = cache
	p.cacheBytes[task.Node] += int64(task.Size)

	if p.shouldFlush(task.Node) {
		p.sendFunc(task.Node, cache)
		p.cacheMap[task.Node] = nil
		p.cacheBytes[task.Node] = 0
		delete(p.cacheTimestamp, task.Node)
	}
}

func (p *ClientPipeline) FlushAll() {
	for node, cache := range p.cacheMap {
		if len(cache) > 0 {
			p.sendFunc(node, cache)
			p.cacheMap[node] = nil
			p.cacheBytes[node] = 0
			delete(p.cacheTimestamp, node)
		}
	}
}

func (p *ClientPipeline) Wait() {
	for _, task := range p.tasks {
		p.processTask(task)
	}
	p.FlushAll()
	p.tasks = p.tasks[:0]
}

// 批量查询 sendFunc
func BatchQuerySendFunc(client interface {
	BatchQueryFiles([]struct {
		NodeName string
		FileHash uint64
	}) ([][]byte, []error)
}) func(node string, tasks []*FileTask) {
	return func(node string, tasks []*FileTask) {
		batch := make([]struct {
			NodeName string
			FileHash uint64
		}, len(tasks))
		for i, t := range tasks {
			batch[i].NodeName = t.Node
			batch[i].FileHash = t.FileHash
		}
		results, errs := client.BatchQueryFiles(batch)
		for i, t := range tasks {
			if errs[i] != nil {
				continue
			}
			t.Data = results[i]
		}
	}
}
