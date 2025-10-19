package network

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// ...existing code...

// TCPClient 维护与单个服务器的长连接
type TCPClient struct {
	serverAddr string
	timeout    time.Duration
	helper     *ProtocolHelper
	Conn       net.Conn
}

// NewTCPClient 创建客户端（尚未连接）
func NewTCPClient(serverAddr string) *TCPClient {
	return &TCPClient{
		serverAddr: serverAddr,
		timeout:    10 * time.Second,
		helper:     &ProtocolHelper{},
	}
}

// SetTimeout 设置读写超时
func (c *TCPClient) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

// Connect 主动建立长连接
func (c *TCPClient) Connect() error {
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
	if c.Conn != nil {
		_ = c.Conn.Close()
		c.Conn = nil
	}
	return nil
}

func (c *TCPClient) ensureConn() (net.Conn, error) {
	if c.Conn != nil {
		return c.Conn, nil
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c.Conn, nil
}

func (c *TCPClient) setWriteDeadline(conn net.Conn) error {
	if c.timeout <= 0 {
		return nil
	}
	//return conn.SetWriteDeadline(time.Now().Add(c.timeout))
	return conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
}

func (c *TCPClient) setReadDeadline(conn net.Conn) error {
	if c.timeout <= 0 {
		return nil
	}
	//return conn.SetReadDeadline(time.Now().Add(c.timeout))
	return conn.SetReadDeadline(time.Now().Add(30 * time.Second))
}

func (c *TCPClient) clearDeadlines(conn net.Conn) {
	if c.timeout <= 0 {
		return
	}
	_ = conn.SetDeadline(time.Time{})
}

// UploadFile 使用持久连接上传
func (c *TCPClient) UploadFile(nodeName, fileName string, fileHash uint64, fileData []byte) error {
	conn, err := c.ensureConn()
	if err != nil {
		return err
	}
	helper := c.helper

	if err := c.setWriteDeadline(conn); err != nil {
		// c.resetConn()
		return err
	}
	if _, err := conn.Write([]byte{1}); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteUint64(conn, fileHash); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteString(conn, fileName, 16); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteString(conn, nodeName, 8); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.WriteUint64(conn, uint64(len(fileData))); err != nil {
		// c.resetConn()
		return err
	}
	if err := helper.SendFileData(conn, fileData); err != nil {
		// c.resetConn()
		return err
	}
	c.clearDeadlines(conn)
	return nil
}

// QueryFile 使用持久连接查询
// 返回服务器返回的文件哈希 (uint64), 文件数据, error
func (c *TCPClient) QueryFile(nodeName string, fileHash uint64) (uint64, []byte, error) {
	conn, err := c.ensureConn()
	if err != nil {
		return 0, nil, err
	}
	helper := c.helper

	if err := c.setWriteDeadline(conn); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	if _, err := conn.Write([]byte{0}); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	if err := helper.WriteString(conn, nodeName, 8); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	if err := helper.WriteUint64(conn, fileHash); err != nil {
		// c.resetConn()
		return 0, nil, err
	}

	if err := c.setReadDeadline(conn); err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	hashBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, hashBuf); err != nil {

		log.Printf("%d", hashBuf)
		// c.resetConn()
		return 0, nil, fmt.Errorf("failed to read file hash from server: %w", err)
	}
	if string(hashBuf) == "00000000" {
		// fmt.Println("failed to read file hash from server")
		return 0, nil, fmt.Errorf("failed to read file hash from server, reveive 00000000")
	}
	returnedHash := binary.BigEndian.Uint64(hashBuf)

	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		log.Printf("%d", lenBuf)
		// c.resetConn()
		return 0, nil, fmt.Errorf("failed to read file length from server: %w", err)
	}

	fileLen := binary.BigEndian.Uint64(lenBuf)
	const MaxFileSize = 100 * 1024 * 1024 // 100MB
	if fileLen == 0 || fileLen > MaxFileSize {
		// c.resetConn()
		return 0, nil, fmt.Errorf("invalid file size: %d", fileLen)
	}
	data, err := helper.ReceiveFileData(conn, fileLen)
	if err != nil {
		// c.resetConn()
		return 0, nil, err
	}
	c.clearDeadlines(conn)
	return returnedHash, data, nil
}

// BatchQueryFiles 使用持久连接批量查询
// 返回每个响应的服务器返回哈希列表, 数据列表, 错误列表
func (c *TCPClient) BatchQueryFiles(tasks []struct {
	NodeName string
	FileHash uint64
}) ([]uint64, [][]byte, []error) {
	results := make([][]byte, len(tasks))
	errs := make([]error, len(tasks))
	hashes := make([]uint64, len(tasks))

	conn, err := c.ensureConn()
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return hashes, results, errs
	}
	helper := c.helper

	// 构建 hash -> index 映射
	idxMap := make(map[uint64]int, len(tasks))
	for i, t := range tasks {
		idxMap[t.FileHash] = i
	}

	// 批量发送请求
	var buf bytes.Buffer
	for _, t := range tasks {
		buf.WriteByte(0)
		nb := []byte(t.NodeName)
		if len(nb) >= 8 {
			buf.Write(nb[:8])
		} else {
			buf.Write(nb)
			buf.Write(make([]byte, 8-len(nb)))
		}
		var hbuf [8]byte
		binary.BigEndian.PutUint64(hbuf[:], t.FileHash)
		buf.Write(hbuf[:])
	}

	// 在一次写入前设置写超时
	if err := c.setWriteDeadline(conn); err != nil {
		for i := range errs {
			errs[i] = err
		}
		return hashes, results, errs
	}

	// 一次性写入并处理短写
	data := buf.Bytes()
	total := len(data)
	written := 0
	for written < total {
		n, err := conn.Write(data[written:])
		if err != nil {
			for i := range errs {
				errs[i] = err
			}
			return hashes, results, errs
		}
		written += n
	}
	// 批量接收响应：按返回的 hash 定位到对应请求并从 idxMap 删除
	const MaxFileSize = 100 * 1024 * 1024
	remaining := len(idxMap)

	for remaining > 0 {
		if err := c.setReadDeadline(conn); err != nil {
			//c.resetConn()
			// 标记尚未收到的为错误
			for _, idx := range idxMap {
				errs[idx] = err
			}
			return hashes, results, errs
		}

		hashBuf := make([]byte, 8)
		if _, err := io.ReadFull(conn, hashBuf); err != nil {
			// 无法继续读取时，将尚未收到的标记为错误并返回
			//c.resetConn()
			for _, idx := range idxMap {
				errs[idx] = fmt.Errorf("failed to read file hash from server: %w", err)
			}
			return hashes, results, errs
		}

		if string(hashBuf) == "00000000" {
			remaining--
			continue
		}
		returnedHash := binary.BigEndian.Uint64(hashBuf)

		lenBuf := make([]byte, 8)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			//c.resetConn()
			for _, idx := range idxMap {
				errs[idx] = fmt.Errorf("failed to read file length from server: %w", err)
			}
			return hashes, results, errs
		}
		fileLen := binary.BigEndian.Uint64(lenBuf)
		if fileLen == 0 || fileLen > MaxFileSize {
			//c.resetConn()
			for _, idx := range idxMap {
				errs[idx] = fmt.Errorf("invalid file size: %d", fileLen)
			}
			return hashes, results, errs
		}
		data, err := helper.ReceiveFileData(conn, fileLen)
		if err != nil {
			//c.resetConn()
			for _, idx := range idxMap {
				errs[idx] = err
			}
			return hashes, results, errs
		}

		// 根据返回的 hash 定位到请求索引（假设每个请求 hash 唯一）
		idx, ok := idxMap[returnedHash]
		if !ok {
			remaining--
			// 非本次请求中的 hash，忽略该响应并继续等待剩余响应
			continue
		}

		hashes[idx] = returnedHash
		results[idx] = data
		errs[idx] = nil

		// 从映射中删除已收到的项
		delete(idxMap, returnedHash)
		remaining--
	}

	c.clearDeadlines(conn)

	// 将仍然留在 idxMap 的索引标为无响应错误（保险）
	for _, idx := range idxMap {
		if errs[idx] == nil {
			errs[idx] = fmt.Errorf("no response from server")
		}
	}

	return hashes, results, errs
}
