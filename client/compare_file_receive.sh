#!/usr/bin/env bash
set -euo pipefail

# 比较 file_receive 下的每个文件与 cid_files 下同名文件是否一致
# 返回值: 0 表示全部相同；1 表示存在差异或缺失；2 表示目录不存在或脚本异常

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CID_DIR="$BASE_DIR/cid_files"
RECV_DIR="$BASE_DIR/file_receive"

if [ ! -d "$RECV_DIR" ]; then
  echo "目录不存在: $RECV_DIR"
  exit 2
fi
if [ ! -d "$CID_DIR" ]; then
  echo "目录不存在: $CID_DIR"
  exit 2
fi

total=0
same=0
diffs=0
missing=0

for path in "$RECV_DIR"/*; do
  [ -e "$path" ] || continue
  # skip directories
  if [ -d "$path" ]; then
    continue
  fi
  total=$((total+1))
  name=$(basename "$path")
  other="$CID_DIR/$name"
  if [ ! -e "$other" ]; then
    echo "[MISSING] $name not found in cid_files"
    missing=$((missing+1))
    continue
  fi
  if cmp -s "$path" "$other"; then
    echo "[SAME]    $name"
    same=$((same+1))
  else
    echo "[DIFF]    $name"
    diffs=$((diffs+1))
    # show sizes and sha256 for debugging
    if command -v stat >/dev/null 2>&1; then
      s1=$(stat -c%s "$path" || echo "?")
      s2=$(stat -c%s "$other" || echo "?")
      echo "  sizes: file_receive=$s1    cid_files=$s2"
    fi
    if command -v sha256sum >/dev/null 2>&1; then
      sha1=$(sha256sum "$path" | awk '{print $1}')
      sha2=$(sha256sum "$other" | awk '{print $1}')
      echo "  sha256: file_receive=$sha1"
      echo "          cid_files    =$sha2"
    fi
  fi
done

echo "---- summary ----"
echo "total files checked: $total"
echo "same: $same"
echo "different: $diffs"
echo "missing in cid_files: $missing"

if [ $diffs -eq 0 ] && [ $missing -eq 0 ]; then
  echo "All OK: all files in file_receive match cid_files"
  exit 0
else
  echo "Some files differ or are missing. See above list."
  exit 1
fi
