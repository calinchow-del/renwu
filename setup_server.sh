#!/bin/bash
# ============================================================
# 服务器一键部署脚本
# 在你的公网服务器上运行:  bash setup_server.sh
# ============================================================

set -e

echo "=========================================="
echo " 预算爬取 Worker 部署脚本"
echo "=========================================="

# 1. 安装系统依赖
echo "[1/5] 安装系统依赖..."
if command -v apt &>/dev/null; then
    apt update -qq && apt install -y -qq python3 python3-pip git screen
elif command -v yum &>/dev/null; then
    yum install -y python3 python3-pip git screen
elif command -v dnf &>/dev/null; then
    dnf install -y python3 python3-pip git screen
fi

# 2. 安装Python依赖
echo "[2/5] 安装Python依赖..."
pip3 install requests beautifulsoup4 lxml openpyxl --quiet 2>/dev/null || \
pip install requests beautifulsoup4 lxml openpyxl --quiet

# 3. 克隆仓库（如果不存在）
echo "[3/5] 克隆仓库..."
WORK_DIR="/root/renwu"
if [ -d "$WORK_DIR" ]; then
    echo "仓库已存在，拉取最新代码..."
    cd "$WORK_DIR"
    git pull origin main
else
    git clone https://github.com/calinchow-del/renwu.git "$WORK_DIR"
    cd "$WORK_DIR"
fi

# 4. 配置Git（用于推送结果）
echo "[4/5] 配置Git..."
git config user.email "worker@budget-scraper.local"
git config user.name "Budget Worker"

# 检查是否能推送（需要认证）
echo ""
echo "=========================================="
echo " 重要：配置Git推送权限"
echo "=========================================="
echo "Worker需要推送结果回GitHub，请确保以下之一："
echo "  方法1: 使用Personal Access Token"
echo "    git remote set-url origin https://<TOKEN>@github.com/calinchow-del/renwu.git"
echo ""
echo "  方法2: 配置SSH key"
echo "    ssh-keygen -t ed25519 && cat ~/.ssh/id_ed25519.pub"
echo "    然后将公钥添加到GitHub Settings -> SSH keys"
echo "    git remote set-url origin git@github.com:calinchow-del/renwu.git"
echo "=========================================="
echo ""

# 5. 启动Worker
echo "[5/5] 启动Worker..."
echo ""
echo "选择运行方式："
echo "  a) 单次执行:    python3 worker.py"
echo "  b) 守护模式:    python3 worker.py --daemon"
echo "  c) screen后台:  screen -dmS worker python3 worker.py --daemon"
echo ""
echo "推荐使用 screen 后台运行："
echo "  screen -dmS worker python3 $WORK_DIR/worker.py --daemon --interval 5"
echo ""
echo "查看运行状态:"
echo "  screen -r worker     # 进入screen查看"
echo "  tail -f worker.log   # 查看日志"
echo ""
echo "部署完成！请先配置Git推送权限，然后启动Worker。"
