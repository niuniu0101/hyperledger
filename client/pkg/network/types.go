package network

import "time"

// AsyncQueryResult 异步查询返回结果
type AsyncQueryResult struct {
	ReturnedHash uint64 // 成功读取到的返回 hash（一般等于请求 hash）
	RequestHash  uint64 // 对应请求的 hash（写入失败等场景用于回退）
	NodeName     string // 对应请求的 node（用于回退中心查询）
	Data         []byte
	Err          error
}

// rawResponse 原始响应数据，由主reader读取后分发给worker处理
type rawResponse struct {
	HashBuf     []byte // 8字节hash
	LenBuf      []byte // 8字节长度
	Data        []byte // 文件数据
	ProofData   []byte // merkle proof raw bytes
	IndicesData []byte // indices bytes
	Err         error  // 读取错误
	Timestamp   time.Time
}

// pendingRequest 记录待响应的请求信息
type pendingRequest struct {
	NodeName     string
	Timestamp    time.Time
	ExpectedSize uint32
}
