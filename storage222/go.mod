module github.com/hyperledger/storage

go 1.23.11

require (
	github.com/cbergoon/merkletree v0.2.0
	github.com/hyperledger/client v0.0.0-00010101000000-000000000000
	github.com/klauspost/reedsolomon v1.12.5
)

require (
	github.com/hyperledger/fabric-gateway v1.8.0 // indirect
	github.com/hyperledger/fabric-protos-go-apiv2 v0.3.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/miekg/pkcs11 v1.1.1 // indirect
	golang.org/x/crypto v0.40.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/grpc v1.75.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/hyperledger/client => /root/go/src/github.com/hyperledger/client
