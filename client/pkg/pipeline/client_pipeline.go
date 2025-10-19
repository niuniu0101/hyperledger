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
}

func NewNodeCacheConfig(defaultSize int) *NodeCacheConfig {
	return &NodeCacheConfig{
		DefaultCacheSize: defaultSize,
		nodeCacheSizes:   make(map[string]int),
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

// ClientPipeline 客户端流水线结构（串行版本）
type ClientPipeline struct {
	cacheMap       map[string][]*FileTask
	cacheConfig    *NodeCacheConfig
	cacheTimestamp map[string]time.Time
	sendFunc       func(node string, tasks []*FileTask)
	hashFunc       HashFunc
	flushTimeout   time.Duration
	tasks          []*FileTask
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

func (p *ClientPipeline) PushWorkload(fileName string, data []byte) {
	task := &FileTask{
		FileName: fileName,
		Data:     data,
		Size:     len(data),
	}
	p.tasks = append(p.tasks, task)
}

func (p *ClientPipeline) shouldFlush(node string) bool {
	cache := p.cacheMap[node]
	if len(cache) >= p.cacheConfig.GetNodeCacheSize(node) {
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

	cache := p.cacheMap[task.Node]
	if len(cache) == 0 {
		p.cacheTimestamp[task.Node] = time.Now()
	}
	cache = append(cache, task)
	p.cacheMap[task.Node] = cache

	if p.shouldFlush(task.Node) {
		p.sendFunc(task.Node, cache)
		p.cacheMap[task.Node] = nil
		delete(p.cacheTimestamp, task.Node)
	}
}

func (p *ClientPipeline) FlushAll() {
	for node, cache := range p.cacheMap {
		if len(cache) > 0 {
			p.sendFunc(node, cache)
			p.cacheMap[node] = nil
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
