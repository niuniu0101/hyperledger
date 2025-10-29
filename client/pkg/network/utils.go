package network

import (
	"net"
	"time"
)

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// helper to set write deadlines and perform full writes can be added here if needed in future
func writeAll(conn net.Conn, buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := conn.Write(buf[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func nowAdd(d time.Duration) time.Time {
	return time.Now().Add(d)
}
