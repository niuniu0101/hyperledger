package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/hyperledger/client/pkg/fabric"
	"github.com/hyperledger/client/pkg/models"
	"github.com/hyperledger/client/pkg/network"
	"github.com/hyperledger/consistent"
)

func buildLocalHashRing(fabricClient *fabric.FabricClient) (*consistent.HashRingManager, error) {
	data, err := fabricClient.GetAllRingsData()
	if err != nil {
		return nil, fmt.Errorf("获取区块链哈希环失败: %v", err)
	}
	var container models.AllRingsContainer
	if err := json.Unmarshal(data, &container); err != nil {
		return nil, fmt.Errorf("解析哈希环数据失败: %v", err)
	}
	hrm := consistent.NewHashRingManager()
	hrm.BuildHashRingsFromData(&container)
	return hrm, nil
}

func displayRingStatus(fabricClient *fabric.FabricClient, title string) {
	fmt.Println("---------------------------------------")
	fmt.Printf("[状态检查] %s\n", title)
	hrm, err := buildLocalHashRing(fabricClient)
	if err != nil {
		log.Fatalf("构建本地哈希环失败: %v", err)
	}
	for ringID, ring := range hrm.GetAllRings() {
		members := ring.GetMembers()
		if len(members) > 0 {
			fmt.Printf("Ring %s 上的节点: ", ringID)
			for _, node := range members {
				fmt.Printf("%s ", node.String())
			}
			fmt.Println()
		} else {
			fmt.Printf("Ring %s 上的节点: (空)\n", ringID)
		}
	}
	fmt.Println("---------------------------------------")
}

func locateFileNode(hrm *consistent.HashRingManager, ringID, fileName string) string {
	return hrm.LocateKey(ringID, fileName)
}

func prepareServerClients(centerClient *network.TCPClient) (map[string]*network.TCPClient, error) {
	clientCache := make(map[string]*network.TCPClient, len(ringToServer)+1)

	if err := centerClient.Connect(); err != nil {
		return nil, fmt.Errorf("中心服务器连接失败: %w", err)
	}
	clientCache[centerServer] = centerClient

	for ring := 0; ring <= 3; ring++ {
		addr, ok := ringToServer[ring]
		if !ok {
			continue
		}
		if _, exists := clientCache[addr]; exists {
			continue
		}
		if addr == centerServer {
			clientCache[addr] = centerClient
			continue
		}
		client := network.NewTCPClient(addr)
		if err := client.Connect(); err != nil {
			return nil, fmt.Errorf("连接 ring%d 服务器(%s) 失败: %w", ring, addr, err)
		}
		clientCache[addr] = client
	}
	return clientCache, nil
}

func closeServerClients(cache map[string]*network.TCPClient) {
	closed := make(map[*network.TCPClient]struct{})
	for addr, client := range cache {
		if client == nil {
			continue
		}
		if _, ok := closed[client]; ok {
			continue
		}
		if err := client.Close(); err != nil {
			fmt.Printf("关闭服务器连接失败(%s): %v\n", addr, err)
		}
		closed[client] = struct{}{}
	}
}
