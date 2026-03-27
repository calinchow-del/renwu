#!/bin/bash
# ============================================================
# Worker守护脚本 - 每5分钟从GitHub拉取指令并执行
# crontab: */5 * * * * bash /root/renwu/worker.sh
# ============================================================

WORK_DIR="/root/renwu"
LOG="$WORK_DIR/logs/worker.log"
LOCK="/tmp/budget_worker.lock"
TASK_FILE="$WORK_DIR/tasks/current_task.json"

mkdir -p "$WORK_DIR/logs" "$WORK_DIR/tasks"

# 防重复
exec 200>"$LOCK"
flock -n 200 || { echo "$(date) worker已在运行" >> "$LOG"; exit 0; }

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG"; }

log "===== Worker启动 ====="

# 确保mihomo运行（GitHub代理）
if ! pgrep -f "mihomo" > /dev/null 2>&1; then
    nohup /root/clash/mihomo -d /root/clash > /root/clash/mihomo.log 2>&1 &
    sleep 3
    log "mihomo已启动"
fi

# 1. 从GitHub拉取最新指令
cd "$WORK_DIR"
git stash 2>/dev/null
git pull origin main >> "$LOG" 2>&1 || {
    log "git pull失败，重试..."
    sleep 2
    git pull origin main >> "$LOG" 2>&1 || log "git pull二次失败"
}
git stash pop 2>/dev/null

# 2. 检查是否有任务文件
if [ -f "$TASK_FILE" ]; then
    TASK_STATUS=$(python3 -c "
import json
with open('$TASK_FILE') as f:
    t = json.load(f)
print(t.get('status',''))
" 2>/dev/null)

    if [ "$TASK_STATUS" = "pending" ]; then
        TASK_CMD=$(python3 -c "
import json
with open('$TASK_FILE') as f:
    t = json.load(f)
print(t.get('command',''))
")
        TASK_ID=$(python3 -c "
import json
with open('$TASK_FILE') as f:
    t = json.load(f)
print(t.get('id','unknown'))
")

        log "执行任务 [$TASK_ID]: $TASK_CMD"

        # 标记为运行中
        python3 -c "
import json, time
with open('$TASK_FILE') as f:
    t = json.load(f)
t['status'] = 'running'
t['started_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
with open('$TASK_FILE','w') as f:
    json.dump(t, f, ensure_ascii=False, indent=2)
"

        # 执行命令
        cd "$WORK_DIR"
        eval "$TASK_CMD" >> "$WORK_DIR/logs/task_${TASK_ID}.log" 2>&1
        EXIT_CODE=$?

        # 标记完成
        python3 -c "
import json, time
with open('$TASK_FILE') as f:
    t = json.load(f)
t['status'] = 'done' if $EXIT_CODE == 0 else 'failed'
t['finished_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
t['exit_code'] = $EXIT_CODE
with open('$TASK_FILE','w') as f:
    json.dump(t, f, ensure_ascii=False, indent=2)
"
        log "任务 [$TASK_ID] 完成, exit=$EXIT_CODE"
    else
        log "任务状态: $TASK_STATUS (非pending，跳过)"
    fi
else
    # 没有任务文件 -> 默认执行爬取
    log "无任务文件，执行默认爬取..."

    # 检查爬取进程是否已在运行
    if pgrep -f "scrape_budgets.py" > /dev/null 2>&1; then
        log "爬取已在运行中，跳过"
    else
        cd "$WORK_DIR"
        python3 scrape_budgets.py --start 1 --end 100 --workers 5 >> "$WORK_DIR/logs/scrape.log" 2>&1 &
        log "爬取已启动 PID: $!"
    fi
fi

# 3. 统计并推送结果
cd "$WORK_DIR"
PDF_COUNT=$(find "$WORK_DIR/预算数据" -name "*.pdf" 2>/dev/null | wc -l)
CITY_DONE=$(python3 -c "
import json, os
f = '$WORK_DIR/scrape_progress.json'
if os.path.exists(f):
    with open(f) as fh:
        p = json.load(fh)
    print(len(p.get('completed',{})))
else:
    print(0)
" 2>/dev/null)

log "统计: 已完成${CITY_DONE}城市, ${PDF_COUNT}个PDF"

# 写入状态文件（方便GitHub查看）
cat > "$WORK_DIR/STATUS.md" << STATUSEOF
# 爬取状态 (自动更新)
- 更新时间: $(date '+%Y-%m-%d %H:%M:%S')
- 已完成城市: ${CITY_DONE}/100
- 已下载PDF: ${PDF_COUNT}
- Worker状态: 运行中
STATUSEOF

# 推送到GitHub
git add -A
git commit -m "auto: ${CITY_DONE}城市完成, ${PDF_COUNT}个PDF ($(date +%H:%M))" 2>/dev/null
git push origin main >> "$LOG" 2>&1 || {
    sleep 2
    git push origin main >> "$LOG" 2>&1 || log "推送失败"
}

log "===== Worker结束 ====="
