module github.com/hyperledger/storage111

go 1.23.11

replace (
	github.com/hyperledger/storage111/chunk => ./chunk
	github.com/hyperledger/storage111/merkleroot => ./merkleroot
	github.com/hyperledger/storage111/pipeline => ./pipeline
)

require github.com/klauspost/reedsolomon v1.12.5

require (
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	golang.org/x/sys v0.30.0 // indirect
)
