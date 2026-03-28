#!/bin/bash
# ============================================================
# Worker守护脚本 v2 - 每5分钟从GitHub拉取指令并推送结果
# crontab: */5 * * * * bash /root/renwu/worker.sh
# 改进: 僵尸进程检测、自动重启、更稳定的git操作
# ============================================================

WORK_DIR="/root/renwu"
LOG="$WORK_DIR/logs/worker.log"
LOCK="/tmp/budget_worker.lock"
TASK_FILE="$WORK_DIR/tasks/current_task.json"
SCRAPE_LOG="$WORK_DIR/logs/scrape.log"

mkdir -p "$WORK_DIR/logs" "$WORK_DIR/tasks"

# 防重复
exec 200>"$LOCK"
flock -w 5 200 || { echo "$(date) worker锁等待超时" >> "$LOG"; exit 0; }

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG"; }

log "===== Worker启动 ====="

# 确保mihomo运行
if ! pgrep -f "mihomo" > /dev/null 2>&1; then
    nohup /root/clash/mihomo -d /root/clash > /root/clash/mihomo.log 2>&1 &
    sleep 3
    log "mihomo已启动"
fi

# 1. 从GitHub拉取最新指令
cd "$WORK_DIR"

# 先stash本地改动，再pull，再pop
git stash --include-untracked -q 2>/dev/null
git pull --rebase origin main >> "$LOG" 2>&1 || {
    git rebase --abort 2>/dev/null
    git pull origin main --no-rebase >> "$LOG" 2>&1 || {
        log "git pull失败，强制重置到远程"
        git fetch origin main 2>/dev/null
        git reset --hard origin/main 2>/dev/null
    }
}
git stash pop -q 2>/dev/null

# 2. 检测僵尸进程（日志超过30分钟没更新 = 进程已死）
is_scraper_alive() {
    local pid=$(pgrep -f "scrape_budgets.py" 2>/dev/null | head -1)
    if [ -z "$pid" ]; then
        echo "dead"
        return
    fi
    # 检查日志文件最后修改时间
    if [ -f "$SCRAPE_LOG" ]; then
        local last_mod=$(stat -c %Y "$SCRAPE_LOG" 2>/dev/null || echo 0)
        local now=$(date +%s)
        local diff=$(( now - last_mod ))
        if [ $diff -gt 1800 ]; then
            log "爬取进程 PID:$pid 已僵死(日志${diff}秒未更新)，杀死"
            kill -9 $pid 2>/dev/null
            sleep 2
            echo "dead"
            return
        fi
    fi
    echo "alive:$pid"
}

SCRAPER_STATE=$(is_scraper_alive)

# 3. 处理任务
if [ -f "$TASK_FILE" ]; then
    TASK_STATUS=$(python3 -c "
import json
with open('$TASK_FILE') as f:
    t = json.load(f)
print(t.get('status',''))
" 2>/dev/null)

    if [ "$TASK_STATUS" = "pending" ]; then
        # 杀掉旧的爬取进程
        pkill -9 -f "scrape_budgets.py" 2>/dev/null
        sleep 2

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
        # 后台执行
        cd "$WORK_DIR"
        nohup bash -c "$TASK_CMD" >> "$WORK_DIR/logs/task_${TASK_ID}.log" 2>&1 &
        log "任务已后台启动 PID: $!"

    elif [ "$TASK_STATUS" = "running" ]; then
        # 如果任务状态是running但进程已死，自动重启
        if [ "$SCRAPER_STATE" = "dead" ]; then
            log "任务标记running但进程已死，自动重启"
            TASK_CMD=$(python3 -c "
import json
with open('$TASK_FILE') as f:
    t = json.load(f)
print(t.get('command',''))
" 2>/dev/null)
            if [ -n "$TASK_CMD" ]; then
                cd "$WORK_DIR"
                nohup bash -c "$TASK_CMD" >> "$WORK_DIR/logs/scrape.log" 2>&1 &
                log "自动重启爬取 PID: $!"
                # 更新启动时间
                python3 -c "
import json, time
with open('$TASK_FILE') as f:
    t = json.load(f)
t['started_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
t['note'] = '自动重启(进程已死)'
with open('$TASK_FILE','w') as f:
    json.dump(t, f, ensure_ascii=False, indent=2)
"
            fi
        else
            log "任务running中，进程存活，跳过"
        fi
    else
        log "任务状态: $TASK_STATUS (跳过)"
    fi
else
    # 没有任务文件 -> 检查是否需要启动默认爬取
    if [ "$SCRAPER_STATE" = "dead" ]; then
        cd "$WORK_DIR"
        nohup python3 scrape_budgets.py --start 1 --end 100 --workers 5 >> "$SCRAPE_LOG" 2>&1 &
        log "默认爬取已启动 PID: $!"
    else
        log "爬取进程运行中，跳过启动"
    fi
fi

# 4. 统计并推送结果
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

# 爬取进程状态
SCRAPE_PID=$(pgrep -f "scrape_budgets.py" 2>/dev/null | head -1)
SCRAPE_STATUS="未运行"
[ -n "$SCRAPE_PID" ] && SCRAPE_STATUS="运行中 (PID: $SCRAPE_PID)"

log "统计: ${CITY_DONE}城市, ${PDF_COUNT}个PDF, 爬取${SCRAPE_STATUS}"

# 写入状态文件
cat > "$WORK_DIR/STATUS.md" << STATUSEOF
# 爬取状态 (自动更新)
- 更新时间: $(date '+%Y-%m-%d %H:%M:%S')
- 已完成城市: ${CITY_DONE}/100
- 已下载PDF: ${PDF_COUNT}
- 爬取进程: ${SCRAPE_STATUS}
- Worker: v2 正常
STATUSEOF

# 推送到GitHub
git add -A
git diff --cached --quiet && { log "无新数据，跳过推送"; log "===== Worker结束 ====="; exit 0; }

git commit -m "auto: ${CITY_DONE}城市完成, ${PDF_COUNT}个PDF ($(date +%H:%M))" 2>/dev/null

for i in 1 2 3 4; do
    git push origin main >> "$LOG" 2>&1 && { log "推送成功"; break; }
    WAIT=$((i * 2))
    log "推送失败，${WAIT}秒后重试..."
    sleep $WAIT
done

log "===== Worker结束 ====="
