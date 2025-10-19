#!/bin/bash
cd /root/go/src/github.com/hyperledger/client
count=$(find file_receive -type f | wc -l)
echo "file_receive文件夹下共有 $count 个文件"