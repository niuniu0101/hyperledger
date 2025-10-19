#!/bin/bash

input_file="./file_to_ring.txt"
output_file="./processed_cids.txt"

# 清空或创建输出文件
> "$output_file"

# 提取CID和ring编号
grep -o 'Qm[a-zA-Z0-9]* -> node[0-9] *(ring[0-9])' "$input_file" | \
sed -E 's/^(Qm[a-zA-Z0-9]*) -> node[0-9] *\(ring([0-9])\)$/\1 \2/' >> "$output_file"

echo "处理完成！结果已保存到 $output_file"