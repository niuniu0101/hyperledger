package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

const (
	DataShards   = 10
	ParityShards = 4
	TotalShards  = DataShards + ParityShards
)

// Shard represents a piece of data returned from a server.
// It includes the data itself and its original index.
type Shard struct {
	Index int
	Data  []byte
}

func main() {
	// 1. 初始化编码器
	enc, err := reedsolomon.New(DataShards, ParityShards)
	if err != nil {
		log.Fatalf("Error creating encoder: %v", err)
	}

	// 准备原始数据
	originalData := []byte("This is a long string that we want to distribute across multiple servers using erasure coding.")
	fmt.Printf("Original Data Length: %d\n", len(originalData))
	fmt.Printf("Original Data: %s\n", string(originalData))

	// 2. 编码并模拟存储到服务器
	shards, err := enc.Split(originalData)
	if err != nil {
		log.Fatalf("Error splitting data: %v", err)
	}
	err = enc.Encode(shards)
	if err != nil {
		log.Fatalf("Error encoding data: %v", err)
	}

	// 使用 map 模拟分布式存储，key 是分片索引（服务器ID），value 是分片数据
	serverStorage := make(map[int][]byte)
	for i, shard := range shards {
		// 复制一份数据，以防原始的 shards 切片被修改
		serverStorage[i] = append([]byte(nil), shard...)
	}
	fmt.Printf("\nData has been encoded and stored on %d simulated servers.\n\n", TotalShards)

	// 3. 模拟从服务器并发获取分片
	fmt.Println("Attempting to fetch shards from servers...")
	// 我们只需要 DataShards 个分片就可以恢复数据
	fetchedShards, err := fetchShardsFromServers(serverStorage)
	if err != nil {
		log.Fatalf("Could not fetch enough shards to reconstruct data: %v", err)
	}

	// 4. 重建数据
	// 验证我们是否真的获取了足够的分片
	// Reconstruct 函数要求一个包含所有分片（包括空位）的完整切片
	reconstructShards := make([][]byte, TotalShards)
	for _, shard := range fetchedShards {
		reconstructShards[shard.Index] = shard.Data
	}

	// 重建丢失的分片
	err = enc.Reconstruct(reconstructShards)
	if err != nil {
		log.Fatalf("Reconstruction failed: %v", err)
	}

	// 将重建后的数据分片合并成原始数据
	var buf bytes.Buffer
	err = enc.Join(&buf, reconstructShards, len(originalData))
	if err != nil {
		log.Fatalf("Could not join shards: %v", err)
	}

	fmt.Println("\nReconstruction successful!")
	fmt.Printf("Reconstructed Data Length: %d\n", buf.Len())
	fmt.Printf("Reconstructed Data: %s\n", buf.String())

	// 验证数据是否一致
	if !bytes.Equal(originalData, buf.Bytes()) {
		fmt.Println("Error: Reconstructed data does not match original data.")
	} else {
		fmt.Println("\nVerification successful: Reconstructed data matches original data.")
	}
}

// fetchShardsFromServers 模拟并发地从服务器获取分片
func fetchShardsFromServers(storage map[int][]byte) ([]Shard, error) {
	// 使用 context 来控制 goroutine 的生命周期
	// 一旦我们收集到足够的分片，就取消其他的 goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shardChannel := make(chan Shard, TotalShards)
	var wg sync.WaitGroup

	for i := 0; i < TotalShards; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// 模拟随机网络延迟
			latency := time.Duration(rand.Intn(100)+20) * time.Millisecond
			
			select {
			case <-time.After(latency):
				// 模拟5%的服务器故障率
				if rand.Intn(100) < 5 {
					fmt.Printf("Server %d failed to respond.\n", index)
					return
				}
				fmt.Printf("Received shard %d from server after %v.\n", index, latency)
				shardChannel <- Shard{Index: index, Data: storage[index]}
			case <-ctx.Done():
				// 如果已经收集到足够的分片，就直接退出
				return
			}
		}(i)
	}

	// 启动一个 goroutine 等待所有服务器的响应（或被取消）
	// 这样可以确保在函数返回后，没有 goroutine 泄露
	go func() {
		wg.Wait()
		close(shardChannel)
	}()

	// 收集最先到达的 DataShards 个分片
	var result []Shard
	for shard := range shardChannel {
		result = append(result, shard)
		if len(result) >= DataShards {
			fmt.Printf("\nCollected %d shards, which is enough to reconstruct. Cancelling other requests.\n", len(result))
			cancel() // 取消其他还在等待的 goroutine
			break
		}
	}
	
	// 如果最终收集到的分片少于需要的分片数量，则返回错误
	if len(result) < DataShards {
		return nil, fmt.Errorf("not enough shards received. Got %d, need %d", len(result), DataShards)
	}

	return result, nil
}