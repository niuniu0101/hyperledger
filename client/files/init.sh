#!/bin/bash

# 生成1000个文件，每个文件约0.8KB
for i in {1..10000}; do
    # 格式化文件名，如 file_0001.txt, file_0002.txt, ..., file_1000.txt
    filename=$(printf "file_%04d.txt" $i)
    
    # 生成约0.8KB的内容（约800字节）
    # 使用base64编码的随机数据来确保内容可读
    head -c 4096 /dev/urandom | base64 | head -c 4096 > "$filename"
    
    # 显示进度
    if [ $((i % 1000)) -eq 0 ]; then
        echo "已创建文件: $filename"
    fi
done

echo "完成！已创建10000个文件，每个文件约4KB"