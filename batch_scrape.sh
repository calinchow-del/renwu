#!/bin/bash
# 分批爬取脚本 - 每批10个城市，避免进程崩溃
# 已完成: 1-4 (上海/北京/深圳/重庆)

cd /root/renwu
LOG="logs/batch_scrape.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG"; echo "$1"; }

log "===== 分批爬取开始 ====="

# 先跑有budget_url的城市 (5-36，排除23/33/37-38等null的)
# 然后跑没有url的城市

# 第一波: 有URL的城市，每批10个
for START in 5 15 25 35; do
  END=$((START + 9))
  [ $END -gt 36 ] && END=36
  log "--- 批次: 城市 $START-$END ---"
  python3 scrape_budgets.py --start $START --end $END --workers 2 2>&1 | tail -5 >> "$LOG"
  RET=$?
  log "批次 $START-$END 完成, 退出码: $RET"
  sleep 5
done

# 第二波: 没有URL的城市，需要探测，每批5个
for START in 37 42 47 52 57 62 67 72 77 82 87 92 97; do
  END=$((START + 4))
  [ $END -gt 100 ] && END=100
  log "--- 批次: 城市 $START-$END (探测模式) ---"
  python3 scrape_budgets.py --start $START --end $END --workers 2 2>&1 | tail -5 >> "$LOG"
  RET=$?
  log "批次 $START-$END 完成, 退出码: $RET"
  sleep 5
done

log "===== 全部批次完成 ====="
