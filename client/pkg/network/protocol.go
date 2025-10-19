package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// WriteByte 写入单字节（与 io.Writer 标准方法兼容）
func (p *ProtocolHelper) WriteByte(b byte) error {
	// 这里需要有目标 Writer，建议实际用法时用 bufio.Writer 或 net.Conn.Write
	return fmt.Errorf("ProtocolHelper.WriteByte 需要指定写入目标")
}

// ReadByte 读取单字节（与 io.Reader 标准方法兼容）
func (p *ProtocolHelper) ReadByte() (byte, error) {
	// 这里需要有目标 Reader，建议实际用法时用 bufio.Reader 或 net.Conn.Read
	return 0, fmt.Errorf("ProtocolHelper.ReadByte 需要指定读取目标")
}

// 协议常量
const (
	NodeNameSize = 8         // 节点名称大小
	HashSize     = 8         // 哈希值大小
	FileSizeSize = 8         // 文件大小字段大小
	BufferSize   = 32 * 1024 // 32KB 传输缓冲区
)

// FileRequest 请求消息结构
type FileRequest struct {
	NodeName string
	FileHash uint64
}

// FileResponse 响应消息结构
type FileResponse struct {
	FileHash uint64
	FileSize uint64
	Content  []byte
}

// ProtocolHelper 协议辅助函数
type ProtocolHelper struct{}

// WriteUint64 写入64位无符号整数（大端序）
func (p *ProtocolHelper) WriteUint64(conn net.Conn, value uint64) error {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, value)
	_, err := conn.Write(buffer)
	return err
}

// ReadUint64 读取64位无符号整数（大端序）
func (p *ProtocolHelper) ReadUint64(conn net.Conn) (uint64, error) {
	buffer := make([]byte, 8)
	_, err := io.ReadFull(conn, buffer)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buffer), nil
}

// WriteString 写入固定长度字符串
func (p *ProtocolHelper) WriteString(conn net.Conn, value string, size int) error {
	buffer := make([]byte, size)
	copy(buffer, []byte(value))
	_, err := conn.Write(buffer)
	return err
}

// ReadString 读取固定长度字符串
func (p *ProtocolHelper) ReadString(conn net.Conn, size int) (string, error) {
	buffer := make([]byte, size)
	_, err := io.ReadFull(conn, buffer)
	if err != nil {
		return "", err
	}
	// 移除null字符
	for i, b := range buffer {
		if b == 0 {
			return string(buffer[:i]), nil
		}
	}
	return string(buffer), nil
}

// SendFileData 发送文件数据
func (p *ProtocolHelper) SendFileData(conn net.Conn, data []byte) error {
	totalWritten := 0
	dataLen := len(data)

	for totalWritten < dataLen {
		remaining := dataLen - totalWritten
		chunkSize := BufferSize
		if remaining < chunkSize {
			chunkSize = remaining
		}

		written, err := conn.Write(data[totalWritten : totalWritten+chunkSize])
		if err != nil {
			return fmt.Errorf("failed to send file data: %w", err)
		}
		totalWritten += written
	}
	return nil
}

// ReceiveFileData 接收文件数据
func (p *ProtocolHelper) ReceiveFileData(conn net.Conn, fileSize uint64) ([]byte, error) {
	data := make([]byte, fileSize)
	totalRead := uint64(0)

	for totalRead < fileSize {
		remaining := fileSize - totalRead
		chunkSize := uint64(BufferSize)
		if remaining < chunkSize {
			chunkSize = remaining
		}

		n, err := io.ReadFull(conn, data[totalRead:totalRead+chunkSize])
		if err != nil {
			return nil, fmt.Errorf("failed to receive file data: %w", err)
		}
		totalRead += uint64(n)
	}
	return data, nil
}
