#!/usr/bin/env python3
"""
服务器端 Worker - 自动拉取任务并执行预算爬取
架构: 沙箱 → GitHub → 服务器主动拉取执行

用法:
  python3 worker.py              # 单次执行
  python3 worker.py --daemon     # 守护模式，每5分钟检查一次
  python3 worker.py --interval 3 # 守护模式，每3分钟检查一次
"""

import json
import os
import sys
import time
import subprocess
import logging
from pathlib import Path
from datetime import datetime

# ========== 配置 ==========
REPO_DIR = Path(__file__).parent
TASKS_DIR = REPO_DIR / "tasks"
OUTPUT_DIR = REPO_DIR / "output"
TASK_FILE = TASKS_DIR / "task.json"
WORKER_LOG = REPO_DIR / "worker.log"
DEFAULT_INTERVAL = 5  # 分钟

# ========== 日志 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(WORKER_LOG, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_cmd(cmd, cwd=None, timeout=120):
    """运行shell命令"""
    try:
        result = subprocess.run(
            cmd, shell=True, cwd=cwd or str(REPO_DIR),
            capture_output=True, text=True, timeout=timeout
        )
        if result.returncode != 0:
            logger.warning(f"命令返回非0: {cmd}\nstderr: {result.stderr[:500]}")
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        logger.error(f"命令超时: {cmd}")
        return -1, "", "timeout"
    except Exception as e:
        logger.error(f"命令执行失败: {cmd}: {e}")
        return -1, "", str(e)


def git_pull():
    """从GitHub拉取最新代码"""
    logger.info("正在从GitHub拉取更新...")
    code, out, err = run_cmd("git pull origin main", timeout=60)
    if code == 0:
        logger.info(f"拉取成功: {out.strip()}")
        return True
    else:
        logger.error(f"拉取失败: {err}")
        return False


def git_push_results():
    """将结果推送回GitHub"""
    logger.info("正在推送结果到GitHub...")

    # 添加所有预算数据和输出
    run_cmd("git add 预算数据/ output/ tasks/ scrape_progress.json scrape.log", timeout=30)

    # 检查是否有变更
    code, out, _ = run_cmd("git diff --cached --stat", timeout=10)
    if not out.strip():
        logger.info("无新变更需要推送")
        return True

    # 提交
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    msg = f"[worker] 自动更新预算数据 {timestamp}"
    code, out, err = run_cmd(f'git commit -m "{msg}"', timeout=30)
    if code != 0:
        logger.error(f"提交失败: {err}")
        return False

    # 推送（带重试）
    for attempt in range(4):
        code, out, err = run_cmd("git push origin main", timeout=60)
        if code == 0:
            logger.info("推送成功!")
            return True
        wait = 2 ** (attempt + 1)
        logger.warning(f"推送失败，{wait}秒后重试... ({err[:200]})")
        time.sleep(wait)

    logger.error("推送最终失败")
    return False


def load_task():
    """加载任务文件"""
    if not TASK_FILE.exists():
        return None
    try:
        with open(TASK_FILE, 'r', encoding='utf-8') as f:
            task = json.load(f)
        return task
    except Exception as e:
        logger.error(f"加载任务文件失败: {e}")
        return None


def update_task_status(task, status, message=""):
    """更新任务状态"""
    task['status'] = status
    task['updated_at'] = datetime.now().isoformat()
    if message:
        task['message'] = message
    with open(TASK_FILE, 'w', encoding='utf-8') as f:
        json.dump(task, f, ensure_ascii=False, indent=2)


def execute_scrape_task(task):
    """执行爬取任务"""
    params = task.get('params', {})
    start = params.get('start_rank', 1)
    end = params.get('end_rank', 100)
    batch_size = params.get('batch_size', 10)

    logger.info(f"开始执行爬取任务: 城市 {start}-{end}, 批次大小 {batch_size}")

    # 分批执行，每批完成后推送一次结果
    current = start
    while current <= end:
        batch_end = min(current + batch_size - 1, end)
        logger.info(f"{'='*50}")
        logger.info(f"正在爬取第 {current}-{batch_end} 名城市...")
        logger.info(f"{'='*50}")

        # 更新任务状态
        update_task_status(task, "running", f"正在爬取城市 {current}-{batch_end}")

        # 执行爬取
        cmd = f"python3 scrape_budgets.py --start {current} --end {batch_end}"
        code, out, err = run_cmd(cmd, timeout=600)  # 10分钟超时每批

        if code == 0:
            logger.info(f"城市 {current}-{batch_end} 爬取完成")
        else:
            logger.warning(f"城市 {current}-{batch_end} 爬取部分失败: {err[:300]}")

        # 每批完成后保存进度并推送
        batch_result = {
            "batch": f"{current}-{batch_end}",
            "completed_at": datetime.now().isoformat(),
            "return_code": code
        }

        # 保存批次结果
        batch_file = OUTPUT_DIR / f"batch_{current:03d}_{batch_end:03d}.json"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_result, f, ensure_ascii=False, indent=2)

        # 推送本批结果
        git_push_results()

        current = batch_end + 1

        # 批次间休息，避免被封
        if current <= end:
            logger.info("批次间休息10秒...")
            time.sleep(10)

    # 任务完成
    update_task_status(task, "completed", f"全部爬取完成: 城市 {start}-{end}")
    git_push_results()
    logger.info("全部任务完成!")


def process_task():
    """处理一个任务周期"""
    # 先拉取最新代码
    if not git_pull():
        return False

    # 加载任务
    task = load_task()
    if not task:
        logger.info("无任务文件")
        return False

    # 检查状态
    status = task.get('status', '')
    if status != 'pending':
        if status == 'running':
            logger.info(f"任务正在执行中: {task.get('message', '')}")
        elif status == 'completed':
            logger.info("任务已完成")
        else:
            logger.info(f"任务状态: {status}")
        return False

    # 执行任务
    logger.info(f"发现新任务: {task.get('task_id')} - {task.get('description')}")
    action = task.get('action', '')

    if action == 'scrape_budgets':
        execute_scrape_task(task)
        return True
    else:
        logger.warning(f"未知任务类型: {action}")
        update_task_status(task, "error", f"未知任务类型: {action}")
        return False


def daemon_mode(interval_minutes):
    """守护进程模式"""
    logger.info(f"Worker 守护模式启动，检查间隔: {interval_minutes}分钟")
    logger.info(f"仓库目录: {REPO_DIR}")

    while True:
        try:
            logger.info(f"--- 检查任务 ({datetime.now().strftime('%H:%M:%S')}) ---")
            process_task()
        except KeyboardInterrupt:
            logger.info("收到中断信号，退出")
            break
        except Exception as e:
            logger.error(f"处理任务时出错: {e}", exc_info=True)

        logger.info(f"等待 {interval_minutes} 分钟后再次检查...")
        time.sleep(interval_minutes * 60)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='预算爬取 Worker')
    parser.add_argument('--daemon', action='store_true', help='守护进程模式')
    parser.add_argument('--interval', type=int, default=DEFAULT_INTERVAL, help=f'检查间隔(分钟), 默认{DEFAULT_INTERVAL}')
    parser.add_argument('--once', action='store_true', help='执行一次就退出')
    args = parser.parse_args()

    # 确保在仓库目录
    os.chdir(str(REPO_DIR))

    if args.daemon:
        daemon_mode(args.interval)
    else:
        process_task()
