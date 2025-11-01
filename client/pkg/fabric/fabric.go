package fabric

import (
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// InitFabricClientFromFlags 通过命令行参数初始化 FabricClient
func InitFabricClientFromFlags() (*FabricClient, error) {
	mspID := flag.String("mspid", "Org1MSP", "MSP ID")
	peerEndpoint := flag.String("peer", "localhost:7051", "Peer endpoint")
	gatewayPeer := flag.String("peerName", "peer0.org1.example.com", "Gateway peer name for TLS")
	// 路径与 simple_test_io/main.go 保持一致
	cryptoBasePath := "../fabric-samples/test-network/organizations/peerOrganizations/org1.example.com"
	certPath := flag.String("cert", cryptoBasePath+"/users/User1@org1.example.com/msp/signcerts", "User cert dir")
	keyPath := flag.String("keyfile", cryptoBasePath+"/users/User1@org1.example.com/msp/keystore", "User key dir")
	tlsCertPath := flag.String("tls", cryptoBasePath+"/peers/peer0.org1.example.com/tls/ca.crt", "Peer TLS cert path")
	channelName := flag.String("channel", "mychannel", "Channel name")
	chaincodeName := flag.String("cc", "multi-ring-manager", "Chaincode name")
	flag.Parse()

	return NewFabricClient(
		*mspID, *certPath, *keyPath, *tlsCertPath,
		*peerEndpoint, *gatewayPeer, *channelName, *chaincodeName,
	)
}

type FabricClient struct {
	gateway  *client.Gateway
	contract *client.Contract
}

// NewFabricClient 创建新的Fabric客户端连接
func NewFabricClient(mspID, certPath, keyPath, tlsCertPath, peerEndpoint, gatewayPeer, channelName, chaincodeName string) (*FabricClient, error) {
	// 创建gRPC连接
	clientConnection, err := newGrpcConnection(tlsCertPath, peerEndpoint, gatewayPeer)
	if err != nil {
		return nil, fmt.Errorf("创建gRPC连接失败: %v", err)
	}

	// 创建身份和签名
	id, err := newIdentity(mspID, certPath)
	if err != nil {
		clientConnection.Close()
		return nil, fmt.Errorf("创建身份失败: %v", err)
	}

	sign, err := newSign(keyPath)
	if err != nil {
		clientConnection.Close()
		return nil, fmt.Errorf("创建签名器失败: %v", err)
	}

	// 连接Gateway
	gw, err := client.Connect(id, client.WithSign(sign), client.WithClientConnection(clientConnection))
	if err != nil {
		clientConnection.Close()
		return nil, fmt.Errorf("连接Gateway失败: %v", err)
	}

	network := gw.GetNetwork(channelName)
	contract := network.GetContract(chaincodeName)

	return &FabricClient{
		gateway:  gw,
		contract: contract,
	}, nil
}

// Close 关闭Fabric连接
func (fc *FabricClient) Close() {
	if fc.gateway != nil {
		fc.gateway.Close()
	}
}

// InitLedger 初始化账本
func (fc *FabricClient) InitLedger() error {
	_, err := fc.contract.SubmitTransaction("InitLedger")
	return err
}

// InitLedgerWithCustomNodes 使用自定义节点配置初始化账本
func (fc *FabricClient) InitLedgerWithCustomNodes() error {
	_, err := fc.contract.SubmitTransaction("InitLedgerWithCustomNodes")
	return err
}

// AddServerNode 向指定环添加服务器节点
func (fc *FabricClient) AddServerNode(ringID, serverName, merkleHash string) error {
	_, err := fc.contract.SubmitTransaction("AddServerNode", ringID, serverName, merkleHash)
	return err
}

// RemoveServerNode 从指定环删除服务器节点
func (fc *FabricClient) RemoveServerNode(ringID, serverName string) error {
	_, err := fc.contract.SubmitTransaction("RemoveServerNode", ringID, serverName)
	return err
}

// UpdateMerkleRoot 更新指定环中节点的Merkle根哈希（异步提交，不等待提交状态）
func (fc *FabricClient) UpdateMerkleRoot(ringID, serverName, newHash string) error {
	// 异步提交事务，避免等待提交状态阶段触发底层 gRPC CommitStatus 调用
	_, _, err := fc.contract.SubmitAsync(
		"UpdateMerkleRoot",
		client.WithArguments(ringID, serverName, newHash),
	)
	return err
}

// GetAllRingsData 获取所有环的数据
func (fc *FabricClient) GetAllRingsData() ([]byte, error) {
	return fc.contract.EvaluateTransaction("GetAllRingsData")
}

// 用于批量更新
type BatchMerkleUpdateItem struct {
	RingID, ServerName, MerkleRootHash string
}

// BatchUpdateMerkleRoots 批量更新指定环中节点的Merkle根哈希
func (fc *FabricClient) BatchUpdateMerkleRoots(updates []BatchMerkleUpdateItem) error {
	j, err := json.Marshal(updates)
	if err != nil {
		return err
	}
	_, _, err = fc.contract.SubmitAsync(
		"BatchUpdateMerkleRoots",
		client.WithArguments(string(j)),
	)
	return err
}

// --- 私有辅助函数 ---

func newGrpcConnection(tlsCertPath, peerEndpoint, gatewayPeer string) (*grpc.ClientConn, error) {
	certificatePEM, err := os.ReadFile(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("无法读取TLS证书文件 %s: %v", tlsCertPath, err)
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certificatePEM) {
		return nil, fmt.Errorf("无法将TLS CA证书附加到池中")
	}
	transportCredentials := credentials.NewClientTLSFromCert(certPool, gatewayPeer)
	connection, err := grpc.NewClient(peerEndpoint, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return nil, fmt.Errorf("无法创建gRPC连接: %v", err)
	}
	return connection, nil
}

func newIdentity(mspID, certPath string) (*identity.X509Identity, error) {
	certificatePEM, err := readFirstFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("无法从 %s 读取证书文件: %v", certPath, err)
	}
	certificate, err := identity.CertificateFromPEM(certificatePEM)
	if err != nil {
		return nil, fmt.Errorf("无法解析身份证书: %v", err)
	}
	id, err := identity.NewX509Identity(mspID, certificate)
	if err != nil {
		return nil, fmt.Errorf("无法创建X.509身份: %v", err)
	}
	return id, nil
}

func newSign(keyPath string) (identity.Sign, error) {
	privateKeyPEM, err := readFirstFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("无法从 %s 读取私钥文件: %v", keyPath, err)
	}
	privateKey, err := identity.PrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("无法解析私钥: %v", err)
	}
	sign, err := identity.NewPrivateKeySign(privateKey)
	if err != nil {
		return nil, fmt.Errorf("无法创建私钥签名器: %v", err)
	}
	return sign, nil
}

func readFirstFile(pathStr string) ([]byte, error) {
	fi, err := os.Stat(pathStr)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		dir, err := os.Open(pathStr)
		if err != nil {
			return nil, err
		}
		defer dir.Close()
		fileNames, err := dir.Readdirnames(1)
		if err != nil || len(fileNames) == 0 {
			return nil, fmt.Errorf("在目录 %s 中找不到文件", pathStr)
		}
		return os.ReadFile(path.Join(pathStr, fileNames[0]))
	}
	// 直接是文件
	return os.ReadFile(pathStr)
}
