#!/bin/bash
# ============================================================
# 服务器一键部署脚本 - 自动配置并启动全量爬取
# 在服务器上运行: bash setup_server.sh
# ============================================================
set -e

WORK_DIR="/root/renwu"
CLASH_DIR="/root/clash"
LOG_DIR="/root/renwu/logs"

echo "=========================================="
echo "  百强城市预算爬取 - 服务器自动部署"
echo "=========================================="

# 1. 安装Python依赖
echo "[1/6] 安装Python依赖..."
pip3 install -q requests beautifulsoup4 lxml openpyxl 2>/dev/null || true

# 2. 配置mihomo代理（用于GitHub推送）
echo "[2/6] 配置mihomo代理..."
mkdir -p "$CLASH_DIR"

if [ ! -f "$CLASH_DIR/mihomo" ]; then
    echo "  下载mihomo..."
    wget -q https://ghfast.top/https://github.com/MetaCubeX/mihomo/releases/download/v1.19.10/mihomo-linux-amd64-v1.19.10.gz -O "$CLASH_DIR/mihomo.gz" 2>/dev/null || \
    wget -q https://mirror.ghproxy.com/https://github.com/MetaCubeX/mihomo/releases/download/v1.19.10/mihomo-linux-amd64-v1.19.10.gz -O "$CLASH_DIR/mihomo.gz" 2>/dev/null
    if [ -f "$CLASH_DIR/mihomo.gz" ]; then
        gzip -d "$CLASH_DIR/mihomo.gz" && chmod +x "$CLASH_DIR/mihomo"
    fi
fi

cat > "$CLASH_DIR/config.yaml" << 'CLASHEOF'
mixed-port: 7890
allow-lan: false
mode: rule
log-level: warning

proxies:
  - name: "HK1"
    type: ss
    server: dns-v2621.api358.com
    port: 7001
    cipher: 2022-blake3-aes-256-gcm
    password: "iNZrhEOivMRU3e8bNqqsdZ2WUk5qe4FmdHkl8rlUIJc=:NDUxNjctYjIyYjUwNmM0MzE0NzFiNmMzOTllZmRjMjE="
  - name: "HK2"
    type: ss
    server: dns-v2621.api358.com
    port: 7002
    cipher: 2022-blake3-aes-256-gcm
    password: "iNZrhEOivMRU3e8bNqqsdZ2WUk5qe4FmdHkl8rlUIJc=:NDUxNjctYjIyYjUwNmM0MzE0NzFiNmMzOTllZmRjMjE="
  - name: "HK3"
    type: ss
    server: dns-v2621.api358.com
    port: 7003
    cipher: 2022-blake3-aes-256-gcm
    password: "iNZrhEOivMRU3e8bNqqsdZ2WUk5qe4FmdHkl8rlUIJc=:NDUxNjctYjIyYjUwNmM0MzE0NzFiNmMzOTllZmRjMjE="

proxy-groups:
  - name: "Proxy"
    type: url-test
    proxies: [HK1, HK2, HK3]
    url: http://www.gstatic.com/generate_204
    interval: 300

rules:
  - DOMAIN-SUFFIX,github.com,Proxy
  - DOMAIN-SUFFIX,githubusercontent.com,Proxy
  - DOMAIN-SUFFIX,githubassets.com,Proxy
  - MATCH,DIRECT
CLASHEOF

# 启动mihomo
if ! pgrep -f "mihomo" > /dev/null 2>&1; then
    nohup "$CLASH_DIR/mihomo" -d "$CLASH_DIR" > "$CLASH_DIR/mihomo.log" 2>&1 &
    sleep 3
    echo "  mihomo已启动"
else
    echo "  mihomo已在运行"
fi

# 3. 修复SSL兼容性：gov.cn改HTTP
echo "[3/6] 修复SSL兼容性..."
cd "$WORK_DIR"
python3 -c "
import json
with open('city_data.json','r') as f:
    data = json.load(f)
for city in data:
    for key in ['website','budget_url']:
        if city.get(key):
            city[key] = city[key].replace('https://','http://')
with open('city_data.json','w') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print('  URL已全部转为HTTP')
"

# 4. 配置Git推送
echo "[4/6] 配置Git..."
mkdir -p "$LOG_DIR"
git config --global user.email "bot@scraper.local"
git config --global user.name "Budget Scraper Bot"
git config --global http.version HTTP/1.1
cd "$WORK_DIR"
git config http.proxy http://127.0.0.1:7890
git config https.proxy http://127.0.0.1:7890

# 5. 设置定时任务
echo "[5/6] 设置定时任务..."
chmod +x "$WORK_DIR/run_task.sh"

# 每30分钟自动提交+推送结果
CRON_PUSH="*/30 * * * * cd $WORK_DIR && git add -A && git commit -m 'auto: update scraped data' 2>/dev/null && git push origin main 2>/dev/null; true"
# mihomo开机自启
CRON_MIHOMO="@reboot nohup $CLASH_DIR/mihomo -d $CLASH_DIR > $CLASH_DIR/mihomo.log 2>&1 &"
(crontab -l 2>/dev/null | grep -v "run_task\|scrape\|mihomo"; echo "$CRON_PUSH"; echo "$CRON_MIHOMO") | crontab -
echo "  定时任务已设置"

# 6. 启动全量爬取
echo "[6/6] 启动全量爬取..."
nohup python3 "$WORK_DIR/scrape_budgets.py" --start 1 --end 100 > "$LOG_DIR/scrape.log" 2>&1 &
SCRAPE_PID=$!

echo ""
echo "=========================================="
echo "  部署完成！爬取已在后台运行"
echo "=========================================="
echo ""
echo "  爬取进程PID: $SCRAPE_PID"
echo "  查看实时进度: tail -f $LOG_DIR/scrape.log"
echo "  查看已下载文件: find $WORK_DIR/预算数据 -name '*.pdf' | wc -l"
echo "  每30分钟自动提交并推送到GitHub"
echo ""
echo "  爬取完成后所有文件会自动出现在GitHub仓库中"
echo "=========================================="
