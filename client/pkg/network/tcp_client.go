package network

import (
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"
)

// TCPClient 维护与单个服务器的长连接
// 添加 mu 来保护 Conn 并与 deadline 操作并发安全
type TCPClient struct {
	serverAddr string
	timeout    time.Duration
	helper     *ProtocolHelper
	Conn       net.Conn
	mu         sync.Mutex
}

// NewTCPClient 创建客户端（尚未连接）
func NewTCPClient(serverAddr string) *TCPClient {
	return &TCPClient{
		serverAddr: serverAddr,
		timeout:    10 * time.Second,
		helper:     &ProtocolHelper{},
	}
}

// ServerAddr 返回服务器地址（用于观测日志）
func (c *TCPClient) ServerAddr() string {
	return c.serverAddr
}

// SetTimeout 设置读写超时
func (c *TCPClient) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

// Connect 主动建立长连接
func (c *TCPClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Conn != nil {
		return nil
	}
	dialer := &net.Dialer{
		Timeout:   c.timeout,
		KeepAlive: 100 * time.Hour,
	}
	conn, err := dialer.Dial("tcp", c.serverAddr)
	if err != nil {
		return err
	}
	c.Conn = conn
	return nil
}

// Close 关闭长连接
func (c *TCPClient) Close() error {
	return c.resetConn()
}

func (c *TCPClient) resetConn() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Conn != nil {
		_ = c.Conn.Close()
		c.Conn = nil
	}
	return nil
}

func (c *TCPClient) ensureConn() (net.Conn, error) {
	c.mu.Lock()
	conn := c.Conn
	c.mu.Unlock()
	if conn != nil {
		return conn, nil
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	conn = c.Conn
	c.mu.Unlock()
	return conn, nil
}

func (c *TCPClient) setWriteDeadline(conn net.Conn) error {
	if c.timeout <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
}

func (c *TCPClient) setReadDeadline(conn net.Conn) error {
	if c.timeout <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return conn.SetReadDeadline(time.Now().Add(120 * time.Second))
}

func (c *TCPClient) clearDeadlines(conn net.Conn) {
	if c.timeout <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = conn.SetDeadline(time.Time{})
}

// UploadFile 使用持久连接上传（使用帧协议）
func (c *TCPClient) UploadFile(nodeName, fileName string, fileHash uint64, fileData []byte) error {
	conn, err := c.ensureConn()
	if err != nil {
		return err
	}
	helper := c.helper

	if err := c.setWriteDeadline(conn); err != nil {
		return err
	}
	// 使用 BuildUploadPayload 构建 payload 并发送为一帧
	payload := helper.BuildUploadPayload(fileHash, nodeName, fileName, fileData)
	if err := helper.WriteFrame(conn, payload); err != nil {
		return err
	}
	c.clearDeadlines(conn)
	return nil
}

// QueryFile 使用持久连接查询（使用帧协议，并可选择验证 merkle proof）
// 如果 verify==true，会尝试使用 fabricClient 获取可信根并验证 proof。
func (c *TCPClient) QueryFile(nodeName string, fileHash uint64, fabricClient interface{}, verify bool) (uint64, []byte, error) {
	conn, err := c.ensureConn()
	if err != nil {
		return 0, nil, err
	}
	helper := c.helper

	if err := c.setWriteDeadline(conn); err != nil {
		return 0, nil, err
	}
	var needProof byte = 0
	if verify {
		needProof = 1
	}
	payload := helper.BuildDownloadPayload(nodeName, fileHash, needProof)
	if err := helper.WriteFrame(conn, payload); err != nil {
		return 0, nil, err
	}

	if err := c.setReadDeadline(conn); err != nil {
		return 0, nil, err
	}
	frame, err := helper.ReadFrame(conn, 200*1024*1024)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read frame: %w", err)
	}
	returnedHash, found, data, proofData, indicesData, perr := helper.ParseStorageResponse(frame)
	if perr != nil {
		return 0, nil, perr
	}
	if !found {
		c.clearDeadlines(conn)
		return returnedHash, nil, fmt.Errorf("not found")
	}

	// If verification requested and proof provided, try to validate
	if verify && len(proofData) > 0 {
		// compute root from proof
		computedRoot, err := helper.VerifyMerkleProof(data, proofData, indicesData)
		if err != nil {
			c.clearDeadlines(conn)
			return 0, nil, fmt.Errorf("failed to compute merkle root from proof: %w", err)
		}
		// attempt to obtain trusted root via fabricClient if provided
		if fabricClient != nil {
			// fabricClient is expected to implement GetServerMerkleRoot(serverName string) (string, error)
			type rootGetter interface {
				GetServerMerkleRoot(string) (string, error)
			}
			if rg, ok := fabricClient.(rootGetter); ok {
				trustedHex, err := rg.GetServerMerkleRoot(nodeName)
				if err != nil {
					c.clearDeadlines(conn)
					return 0, nil, fmt.Errorf("failed to fetch trusted root from fabric: %w", err)
				}
				trustedBytes, err := hex.DecodeString(trustedHex)
				if err != nil {
					c.clearDeadlines(conn)
					return 0, nil, fmt.Errorf("invalid trusted root hex: %w", err)
				}
				if !bytesEqual(trustedBytes, computedRoot) {
					c.clearDeadlines(conn)
					return 0, nil, fmt.Errorf("merkle proof verification failed: computed root %x != trusted root %x", computedRoot, trustedBytes)
				}
			}
		}
	}

	c.clearDeadlines(conn)
	return returnedHash, data, nil
}
