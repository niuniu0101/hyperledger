#!/bin/bash

# 定义要检查的端口范围（8080到8089，共10个端口）
PORTS=(8080 8081 8082 8083 8084 8085 8086 8087 8088 8089)

# 遍历每个端口，检查并杀死占用进程
for port in "${PORTS[@]}"; do
    echo "检查端口: $port"
    
    # 使用lsof查找占用端口的进程PID（如果存在）
    # -i :$port 表示查找该端口的网络连接
    # -t 只输出PID
    pid=$(lsof -i :$port -t)
    
    if [ -n "$pid" ]; then
        # 找到进程，显示进程信息
        echo "  端口 $port 被进程 $pid 占用，进程信息："
        ps -p $pid -o comm,user  # 显示进程名和用户
        
        # 杀死进程（-9表示强制终止）
        echo "  正在杀死进程 $pid..."
        kill -9 $pid
        
        # 验证是否杀死成功
        if [ $? -eq 0 ]; then
            echo "  进程 $pid 已成功杀死"
        else
            echo "  警告：杀死进程 $pid 失败"
        fi
    else
        echo "  端口 $port 未被占用"
    fi
    echo "-------------------------"
done

echo "所有端口处理完毕"