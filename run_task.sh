#!/bin/bash
# ============================================================
# 自动爬取任务脚本 - 由cron或手动调用
# ============================================================

WORK_DIR="/root/renwu"
LOG_DIR="$WORK_DIR/logs"
LOCK_FILE="/tmp/budget_scraper.lock"

# 防止重复运行
if [ -f "$LOCK_FILE" ]; then
    LOCK_PID=$(cat "$LOCK_FILE")
    if kill -0 "$LOCK_PID" 2>/dev/null; then
        echo "$(date) - 爬取任务正在运行中 (PID: $LOCK_PID)，跳过"
        exit 0
    fi
fi
echo $$ > "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

cd "$WORK_DIR"
mkdir -p "$LOG_DIR"

echo "$(date) ====== 开始执行爬取任务 ======"

# 1. 确保mihomo在运行
if ! pgrep -f "mihomo" > /dev/null 2>&1; then
    echo "$(date) 启动mihomo..."
    nohup /root/clash/mihomo -d /root/clash > /root/clash/mihomo.log 2>&1 &
    sleep 3
fi

# 2. 从GitHub拉取最新代码
echo "$(date) 拉取最新代码..."
cd "$WORK_DIR"
git pull origin main 2>/dev/null || true

# 3. 检查爬取进度，决定从哪里继续
if [ -f "$WORK_DIR/scrape_progress.json" ]; then
    COMPLETED=$(python3 -c "
import json
with open('scrape_progress.json') as f:
    p = json.load(f)
print(len(p.get('completed_cities',[])))
" 2>/dev/null || echo "0")
    echo "$(date) 已完成 $COMPLETED 个城市"
else
    COMPLETED=0
fi

# 4. 如果还没全部完成，继续爬取
if [ "$COMPLETED" -lt 100 ]; then
    echo "$(date) 继续爬取..."
    python3 "$WORK_DIR/scrape_budgets.py" --start 1 --end 100 >> "$LOG_DIR/scrape.log" 2>&1
    echo "$(date) 爬取完成"
else
    echo "$(date) 全部100个城市已完成"
fi

# 5. 提交并推送结果到GitHub
echo "$(date) 提交并推送结果..."
cd "$WORK_DIR"
git add -A
git commit -m "auto: 更新预算爬取数据 ($(date +%Y%m%d_%H%M))" 2>/dev/null || true
git push origin main 2>/dev/null || {
    echo "$(date) 推送失败，等2秒重试..."
    sleep 2
    git push origin main 2>/dev/null || {
        sleep 4
        git push origin main 2>/dev/null || echo "$(date) 推送失败，下次重试"
    }
}

# 6. 统计
PDF_COUNT=$(find "$WORK_DIR/预算数据" -name "*.pdf" 2>/dev/null | wc -l)
HTML_COUNT=$(find "$WORK_DIR/预算数据" -name "*.html" -not -name "*.gitkeep" 2>/dev/null | wc -l)
echo "$(date) 统计: PDF=$PDF_COUNT, HTML=$HTML_COUNT, 已完成城市=$COMPLETED"
echo "$(date) ====== 任务执行完毕 ======"
