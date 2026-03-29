#!/bin/bash
# 每小时自动调优循环：爬取 -> 校验 -> 监控 -> 提交
# 运行方式: nohup bash auto_tune_loop.sh >> logs/auto_tune.log 2>&1 &

cd "$(dirname "$0")"
LOGDIR="logs"
mkdir -p "$LOGDIR"

MAX_ROUNDS=20
ROUND=0

echo "============================================"
echo "自动调优循环启动: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

while [ $ROUND -lt $MAX_ROUNDS ]; do
    ROUND=$((ROUND + 1))
    echo ""
    echo "====== 第 ${ROUND} 轮 $(date '+%Y-%m-%d %H:%M:%S') ======"

    # Step 1: 爬取弱城市 (40分钟超时)
    echo "[${ROUND}] 开始爬取弱城市..."
    timeout 2400 python3 scrape_v12.py --retry-weak --force 2>&1 | tail -20
    echo "[${ROUND}] 爬取完成: $(date '+%H:%M:%S')"

    # Step 2: PDF校验
    echo "[${ROUND}] 开始PDF校验..."
    python3 validate_pdfs.py 2>&1 | tail -30
    echo "[${ROUND}] 校验完成: $(date '+%H:%M:%S')"

    # Step 3: 监控报告
    echo "[${ROUND}] 生成监控报告..."
    python3 monitor.py 2>&1 | tail -15

    # Step 4: 提交
    echo "[${ROUND}] 提交代码..."
    git add -A 预算数据/ scrape_progress_v12.json scrape_v12.py monitor.py validate_pdfs.py STATUS.md logs/monitor_report.txt 2>/dev/null
    git commit -m "[auto-tune] round ${ROUND} $(date '+%m-%d %H:%M') 调优+校验+清理" 2>/dev/null || true
    git push origin main 2>/dev/null || true

    # 检查是否全部达标
    GOOD=$(python3 -c "
import json
p = json.load(open('scrape_progress_v12.json'))
c = p.get('completed', {})
good = sum(1 for v in c.values() if isinstance(v, dict) and v.get('found', 0) >= 15)
print(good)
" 2>/dev/null || echo 0)
    echo "[${ROUND}] 当前达标城市: ${GOOD}/100"

    if [ "$GOOD" -ge 100 ]; then
        echo "所有城市已达标! 停止循环。"
        break
    fi

    # 等到下一小时
    echo "[${ROUND}] 等待到下一轮... (sleep 3600s)"
    sleep 3600
done

echo "============================================"
echo "自动调优循环结束: $(date '+%Y-%m-%d %H:%M:%S')"
echo "总计运行 ${ROUND} 轮"
echo "============================================"
