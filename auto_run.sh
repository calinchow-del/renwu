#!/bin/bash
# 自动爬取+监控+调优闭环脚本
# 由cron定时触发, 每轮:
#   1. 跑一批城市爬取
#   2. 运行监控报告
#   3. 根据报告决定下一步策略
#   4. 推送结果到GitHub

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

ROUND_LOG="$LOG_DIR/auto_run_$(date +%Y%m%d_%H%M%S).log"
LOCK_FILE="$SCRIPT_DIR/.auto_run.lock"

# 防止并发
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE" 2>/dev/null)
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
        echo "$(date) 另一个实例正在运行 (PID: $PID), 退出" | tee -a "$ROUND_LOG"
        exit 0
    fi
    echo "$(date) 发现过期锁文件, 清理" | tee -a "$ROUND_LOG"
fi
echo $$ > "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$ROUND_LOG"
}

log "========== 自动爬取轮次开始 =========="

# 安装���赖(首次)
python3 -m pip install -q requests beautifulsoup4 lxml openpyxl 2>/dev/null || apt install -y python3-requests python3-bs4 python3-lxml python3-openpyxl 2>/dev/null || true

# Step 1: 运行监控, 判断当前状态
log "Step 1: 运行监控报告..."
MONITOR_ACTION=$(python3 "$SCRIPT_DIR/monitor.py" 2>&1 | tee -a "$ROUND_LOG" | tail -1 | grep -oP '(?<=建议操作: )\w+' || echo "RUN_FULL")
log "监控建议: $MONITOR_ACTION"

# Step 2: 根据建议执行爬取
case "$MONITOR_ACTION" in
    "ALL_GOOD")
        log "所有城市已达标! 无需爬取."
        ;;
    "RETRY_WEAK")
        log "Step 2: 重试弱城市..."
        timeout 3600 python3 "$SCRIPT_DIR/scrape_v12.py" --retry-weak --force 2>&1 | tee -a "$ROUND_LOG" || true
        ;;
    "RUN_FULL"|*)
        log "Step 2: 全量爬取..."
        timeout 3600 python3 "$SCRIPT_DIR/scrape_v12.py" --start 1 --end 100 2>&1 | tee -a "$ROUND_LOG" || true
        ;;
esac

# Step 3: 再次监控, 生成最新报告
log "Step 3: 生成最终报告..."
python3 "$SCRIPT_DIR/monitor.py" 2>&1 | tee -a "$ROUND_LOG" || true

# Step 4: 更新STATUS.md
PROGRESS_FILE="$SCRIPT_DIR/scrape_progress_v12.json"
if [ -f "$PROGRESS_FILE" ]; then
    COMPLETED=$(python3 -c "
import json
with open('$PROGRESS_FILE') as f:
    d = json.load(f)
c = d.get('completed', {})
good = sum(1 for v in c.values() if isinstance(v, dict) and v.get('found', 0) >= 15)
total = len(c)
pdfs = sum(v.get('downloaded', 0) for v in c.values() if isinstance(v, dict))
print(f'{good}/{total} 达标, {pdfs} PDF')
" 2>/dev/null || echo "统计失败")
else
    COMPLETED="进度文件不存在"
fi

cat > "$SCRIPT_DIR/STATUS.md" << EOF
# 爬取状态 (自动更新)
- 更新时间: $(date '+%Y-%m-%d %H:%M:%S')
- 进度: $COMPLETED
- 爬取版本: v12
- 自动轮次: $(ls "$LOG_DIR"/auto_run_*.log 2>/dev/null | wc -l)
- 最后操作: $MONITOR_ACTION
EOF

# Step 5: Git推送 (如果有remote)
if git remote -v 2>/dev/null | grep -q origin; then
    log "Step 5: 推送到GitHub..."
    git add -A 预算数据/ scrape_progress_v12.json STATUS.md logs/monitor_report.txt 2>/dev/null || true
    if git diff --cached --quiet 2>/dev/null; then
        log "无新变更"
    else
        git commit -m "[auto] 预算爬取更新 $(date '+%m-%d %H:%M') - $COMPLETED" 2>/dev/null || true
        git push origin main 2>/dev/null && log "推送成功" || log "推送失败"
    fi
fi

log "========== 自动爬取轮次结束 =========="

# 清理旧日志(保留最近20个)
ls -t "$LOG_DIR"/auto_run_*.log 2>/dev/null | tail -n +21 | xargs rm -f 2>/dev/null || true
