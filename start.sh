#!/bin/bash

# 应用名称
APP_NAME="canada-api-0.0.1-SNAPSHOT.jar"

# 计算 GC 线程参数
TOTAL_CORES=$(nproc)         # 总逻辑核（如 96）
SERVICE_COUNT=3              # 总服务数（改为部署的项目总数）
CORES_PER_SERVICE=$((TOTAL_CORES / SERVICE_COUNT))

PARALLEL_GC_THREADS=$((CORES_PER_SERVICE / 2))     # 通常设为一半
CONC_GC_THREADS=$((PARALLEL_GC_THREADS / 2))     # 一半的并发标记线程

echo "检测到总核数: ${TOTAL_CORES}"
echo "为每个服务分配约 ${CORES_PER_SERVICE} 个线程"
echo "GC 配置: ParallelGCThreads=${PARALLEL_GC_THREADS}, ConcGCThreads=${CONC_GC_THREADS}"

# JVM 参数配置
JVM_OPTS="
-Duser.timezone=Asia/Shanghai \
-Xms24g -Xmx32g -XX:MaxRAMPercentage=80.0 \
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=200 \
-XX:+ParallelRefProcEnabled \
-XX:+UnlockExperimentalVMOptions \
-XX:+UseStringDeduplication \
-XX:+AlwaysPreTouch \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=logs/heapdump.hprof \
-Xlog:gc*:file=logs/gc.log:time,uptime,level,tags:filecount=5,filesize=50M \
-XX:ParallelGCThreads=${PARALLEL_GC_THREADS} \
-XX:ConcGCThreads=${CONC_GC_THREADS} \
-XX:InitiatingHeapOccupancyPercent=30"

# 日志目录
LOG_DIR="logs"

if [ ! -d "$LOG_DIR" ]; then
  echo "日志目录 $LOG_DIR 不存在，正在创建..."
  mkdir -p "$LOG_DIR"
fi

# 查找并停止旧进程
PID=$(ps -ef | grep "$APP_NAME" | grep -v grep | awk '{print $2}')
if [ -n "$PID" ]; then
  echo "检测到应用正在运行，PID: $PID，准备停止..."
  kill $PID
  COUNT=0
  while kill -0 $PID 2>/dev/null; do
    sleep 1
    COUNT=$((COUNT + 1))
    if [ $COUNT -ge 30 ]; then
      echo "进程 $PID 停止超时，强制杀死"
      kill -9 $PID
      break
    fi
  done
  echo "旧进程已停止"
else
  echo "未检测到正在运行的应用，准备启动"
fi

# 启动应用
echo "启动应用：$APP_NAME"
nohup java $JVM_OPTS -jar $APP_NAME > /dev/null 2>&1 &

echo "应用已后台启动，PID: $!"
