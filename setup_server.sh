#!/bin/bash
# ============================================================
# 一键部署 - 粘贴到服务器即可，全自动运行
# ============================================================
set -e

WORK_DIR="/root/renwu"
CLASH_DIR="/root/clash"

echo "========== 部署开始 =========="

# 1. 依赖
echo "[1/5] 依赖..."
pip3 install -q requests beautifulsoup4 lxml openpyxl 2>/dev/null || true

# 2. mihomo
echo "[2/5] 代理..."
mkdir -p "$CLASH_DIR"
if [ ! -f "$CLASH_DIR/mihomo" ]; then
    wget -q https://ghfast.top/https://github.com/MetaCubeX/mihomo/releases/download/v1.19.10/mihomo-linux-amd64-v1.19.10.gz -O "$CLASH_DIR/mihomo.gz" 2>/dev/null && \
    gzip -d "$CLASH_DIR/mihomo.gz" && chmod +x "$CLASH_DIR/mihomo"
fi

cat > "$CLASH_DIR/config.yaml" << 'EOF'
mixed-port: 7890
allow-lan: false
mode: rule
log-level: warning
proxies:
  - {name: HK1, type: ss, server: dns-v2621.api358.com, port: 7001, cipher: 2022-blake3-aes-256-gcm, password: "iNZrhEOivMRU3e8bNqqsdZ2WUk5qe4FmdHkl8rlUIJc=:NDUxNjctYjIyYjUwNmM0MzE0NzFiNmMzOTllZmRjMjE="}
  - {name: HK2, type: ss, server: dns-v2621.api358.com, port: 7002, cipher: 2022-blake3-aes-256-gcm, password: "iNZrhEOivMRU3e8bNqqsdZ2WUk5qe4FmdHkl8rlUIJc=:NDUxNjctYjIyYjUwNmM0MzE0NzFiNmMzOTllZmRjMjE="}
  - {name: HK3, type: ss, server: dns-v2621.api358.com, port: 7003, cipher: 2022-blake3-aes-256-gcm, password: "iNZrhEOivMRU3e8bNqqsdZ2WUk5qe4FmdHkl8rlUIJc=:NDUxNjctYjIyYjUwNmM0MzE0NzFiNmMzOTllZmRjMjE="}
proxy-groups:
  - {name: Proxy, type: url-test, proxies: [HK1, HK2, HK3], url: "http://www.gstatic.com/generate_204", interval: 300}
rules:
  - DOMAIN-SUFFIX,github.com,Proxy
  - DOMAIN-SUFFIX,githubusercontent.com,Proxy
  - DOMAIN-SUFFIX,githubassets.com,Proxy
  - MATCH,DIRECT
EOF

pgrep -f mihomo || nohup "$CLASH_DIR/mihomo" -d "$CLASH_DIR" > "$CLASH_DIR/mihomo.log" 2>&1 &
sleep 2

# 3. Git配置
echo "[3/5] Git..."
git config --global user.email "bot@scraper.local"
git config --global user.name "Budget Bot"
git config --global http.version HTTP/1.1
cd "$WORK_DIR"
git config http.proxy http://127.0.0.1:7890
git config https.proxy http://127.0.0.1:7890
git stash 2>/dev/null
git pull origin main || true
git stash pop 2>/dev/null

# 4. 修复URL为HTTP
echo "[4/5] 修复URL..."
python3 -c "
import json
with open('city_data.json') as f: data=json.load(f)
for c in data:
    for k in ['website','budget_url']:
        if c.get(k): c[k]=c[k].replace('https://','http://')
with open('city_data.json','w') as f: json.dump(data,f,ensure_ascii=False,indent=2)
"

# 5. 设置cron: 每5分钟执行worker
echo "[5/5] 定时任务..."
mkdir -p "$WORK_DIR/logs"
chmod +x "$WORK_DIR/worker.sh"
CRON="*/5 * * * * /bin/bash $WORK_DIR/worker.sh"
BOOT="@reboot nohup $CLASH_DIR/mihomo -d $CLASH_DIR > $CLASH_DIR/mihomo.log 2>&1 &"
(crontab -l 2>/dev/null | grep -v "worker\|mihomo\|scrape"; echo "$CRON"; echo "$BOOT") | crontab -

echo ""
echo "========== 部署完成 =========="
echo ""
echo "  定时任务: 每5分钟自动拉取GitHub指令并执行"
echo "  查看日志: tail -f $WORK_DIR/logs/worker.log"
echo "  查看爬取: tail -f $WORK_DIR/logs/scrape.log"
echo ""
echo "  现在立即启动第一次爬取..."

# 立即执行一次
bash "$WORK_DIR/worker.sh" &
echo "  Worker已启动 (PID: $!)"
echo "  GitHub仓库会自动更新进度"
