package network

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

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

// Frame header: 4 bytes
// header[0] = magic (0xA5)
// header[1..3] = payload length (big-endian 3 bytes)
const (
	FrameMagic byte = 0xA5
	Max3Byte   int  = 0xFFFFFF
)

// WriteFrame 写入一个完整的消息帧（header + payload），处理短写
func (p *ProtocolHelper) WriteFrame(conn net.Conn, payload []byte) error {
	if len(payload) > Max3Byte {
		return fmt.Errorf("payload too large: %d", len(payload))
	}
	header := make([]byte, 4)
	header[0] = FrameMagic
	header[1] = byte((len(payload) >> 16) & 0xFF)
	header[2] = byte((len(payload) >> 8) & 0xFF)
	header[3] = byte(len(payload) & 0xFF)

	// write header
	if _, err := conn.Write(header); err != nil {
		return err
	}
	// write payload handling short writes
	total := len(payload)
	written := 0
	for written < total {
		n, err := conn.Write(payload[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

// ReadFrame 读取一个完整帧并返回 payload（会阻塞直到读满或出错）
func (p *ProtocolHelper) ReadFrame(conn net.Conn, maxAccept int) ([]byte, error) {
	// Scan until we find the frame magic byte. This allows skipping any
	// spurious bytes on the stream and resynchronizing to the next frame.
	// We read one byte at a time until we see FrameMagic, then read the
	// remaining 3 header bytes to obtain payload length.
	var one [1]byte
	for {
		if _, err := io.ReadFull(conn, one[:]); err != nil {
			return nil, err
		}
		if one[0] == FrameMagic {
			break
		}
		log.Printf("magic number not equal 0xA5, continueing")
		// otherwise keep scanning
	}

	// read remaining 3 bytes of header
	rest := make([]byte, 3)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return nil, err
	}
	payloadLen := int(rest[0])<<16 | int(rest[1])<<8 | int(rest[2])
	if payloadLen == 0 {
		return nil, nil
	}
	if maxAccept > 0 && payloadLen > maxAccept {
		return nil, fmt.Errorf("payload too large: %d", payloadLen)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// BuildDownloadPayload 构建下载请求的 payload
func (p *ProtocolHelper) BuildDownloadPayload(nodeName string, fileHash uint64, needProof byte) []byte {
	// opType=1
	buf := make([]byte, 0, 1+8+8+1)
	buf = append(buf, byte(1))
	// nodeName 8 bytes
	nb := make([]byte, NodeNameSize)
	copy(nb, []byte(nodeName))
	buf = append(buf, nb...)
	// fileHash 8 bytes
	var hbuf [8]byte
	binary.BigEndian.PutUint64(hbuf[:], fileHash)
	buf = append(buf, hbuf[:]...)
	// needProof 1 byte
	buf = append(buf, needProof)
	return buf
}

// BuildUploadPayload 构建上传请求的 payload (op=0)
func (p *ProtocolHelper) BuildUploadPayload(fileHash uint64, nodeName string, filename string, fileData []byte) []byte {
	// opType=0
	// op(1) + fileHash(8) + nodeName(8) + filename(16) + fileLen(4) + fileData
	fileLen := uint32(len(fileData))
	buf := make([]byte, 0, 1+8+8+16+4+len(fileData))
	buf = append(buf, byte(0))
	var hbuf [8]byte
	binary.BigEndian.PutUint64(hbuf[:], fileHash)
	buf = append(buf, hbuf[:]...)
	nN := make([]byte, 8)
	copy(nN, []byte(nodeName))
	buf = append(buf, nN...)
	fb := make([]byte, 16)
	copy(fb, []byte(filename))
	buf = append(buf, fb...)
	var lenb [4]byte
	binary.BigEndian.PutUint32(lenb[:], fileLen)
	buf = append(buf, lenb[:]...)
	buf = append(buf, fileData...)
	return buf
}

// ParseStorageResponse 解析存储层响应 payload
// 返回: fileHash, found, fileData(if found), proofData, indicesData, error
func (p *ProtocolHelper) ParseStorageResponse(payload []byte) (uint64, bool, []byte, []byte, []byte, error) {
	// payload starts with fileHash(8) + found(1)
	if len(payload) < 9 {
		return 0, false, nil, nil, nil, fmt.Errorf("payload too short")
	}
	fileHash := binary.BigEndian.Uint64(payload[0:8])
	found := payload[8]
	idx := 9
	if found == 0 {
		return fileHash, false, nil, nil, nil, nil
	}
	if len(payload) < idx+4 {
		return fileHash, false, nil, nil, nil, fmt.Errorf("payload too short for fileLen")
	}
	fileLen := int(binary.BigEndian.Uint32(payload[idx : idx+4]))
	idx += 4
	if len(payload) < idx+fileLen {
		return fileHash, false, nil, nil, nil, fmt.Errorf("payload too short for fileData")
	}
	fileData := payload[idx : idx+fileLen]
	idx += fileLen
	// proofFlag
	if len(payload) == idx {
		return fileHash, true, fileData, nil, nil, nil
	}
	proofFlag := payload[idx]
	idx++
	var proofData []byte
	var indicesData []byte
	if proofFlag == 1 {
		if len(payload) < idx+2 {
			return fileHash, true, fileData, nil, nil, fmt.Errorf("payload too short for merklePathLen")
		}
		merkleLen := int(binary.BigEndian.Uint16(payload[idx : idx+2]))
		indicesLen := merkleLen
		merkleLen *= 32
		idx += 2
		if len(payload) < idx+merkleLen {
			return fileHash, true, fileData, nil, nil, fmt.Errorf("payload too short for merklePathData")
		}
		proofData = payload[idx : idx+merkleLen]
		idx += merkleLen
		//if len(payload) < idx+2 {
		//	return fileHash, true, fileData, proofData, nil, fmt.Errorf("payload too short for indicesLen")
		//}
		//indicesLen := int(binary.BigEndian.Uint16(payload[idx : idx+2]))
		//idx += 2

		if len(payload) < idx+indicesLen {
			return fileHash, true, fileData, proofData, nil, fmt.Errorf("payload too short for indicesData")
		}
		indicesData = payload[idx : idx+indicesLen]
		idx += indicesLen
	}
	return fileHash, true, fileData, proofData, indicesData, nil
}

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

// VerifyMerkleProof 根据给定的叶子数据（内容 data）以及 proofData/indicesData 重新计算 Merkle root。
// 约定：
//   - proofData 是若干个 sibling 哈希依次连在一起，每个 sibling 长度为 sha256.Size（32）字节。
//   - indicesData 是与 proofData 元素一一对应的字节序列，值为 1 表示 sibling 在右侧（当前为左），0 表示 sibling 在左侧（当前为右）。
//
// 返回：计算出的 Merkle root（字节切片），或 error（例如数据格式异常）。
func (p *ProtocolHelper) VerifyMerkleProof(data []byte, proofData []byte, indicesData []byte) ([]byte, error) {
	if len(proofData) == 0 {
		// 没有证明，返回 leaf hash 作为 root（单节点树）
		h := sha256.Sum256(data)
		return h[:], nil
	}
	const hlen = 32
	if len(proofData)%hlen != 0 {
		return nil, fmt.Errorf("invalid proofData length: %d", len(proofData))
	}
	steps := len(proofData) / hlen
	if len(indicesData) != steps {
		return nil, fmt.Errorf("indicesData length %d != proof steps %d", len(indicesData), steps)
	}

	// 叶子哈希：按照示例使用原始 data 的 sha256
	curHashRaw := sha256.Sum256(data)
	fmt.Printf("数据如下%x\n", data[:12])
	curHash := make([]byte, len(curHashRaw))
	copy(curHash, curHashRaw[:])
	fmt.Printf("当前哈希如下leaf hash      %x\n", curHash)

	for i := 0; i < steps; i++ {
		sibRaw := proofData[i*hlen : (i+1)*hlen]
		sib := make([]byte, len(sibRaw))
		copy(sib, sibRaw)
		fmt.Printf("[PROOF-DEBUG] step %d sibling (sib) %x\n", i, sib)
		var combined []byte
		if indicesData[i] == 1 {
			fmt.Printf("111\n")
			combined = make([]byte, 0, len(curHash)+len(sib))
			combined = append(combined, curHash...)
			combined = append(combined, sib...)
		} else {
			fmt.Printf("222\n")
			combined = make([]byte, 0, len(curHash)+len(sib))
			combined = append(combined, sib...)
			combined = append(combined, curHash...)
		}
		hh := sha256.Sum256(combined)
		nextCur := make([]byte, len(hh))
		copy(nextCur, hh[:])
		curHash = nextCur
		fmt.Printf("当前哈希如下after level %d: %x\n", i, curHash)
	}
	// curHash is computed root
	out := make([]byte, len(curHash))
	copy(out, curHash)
	return out, nil
}
