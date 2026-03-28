#!/usr/bin/env python3
"""监控报告生成器 - 统计预算爬取进度"""

import json
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent / "预算数据"
CITY_DATA_FILE = Path(__file__).parent / "city_data.json"
REPORT_FILE = Path(__file__).parent / "logs" / "monitor_report.txt"
STATUS_FILE = Path(__file__).parent / "STATUS.md"

TARGET_DEPTS = [
    "卫生健康委员会", "教育局", "发展和改革局", "规划和自然资源局",
    "交通运输局", "科技创新局", "水务局", "人力资源和社会保障局",
    "工业和信息化局", "市场监督管理局", "国有资产监督管理委员会",
    "公安局", "公安局交通警察局", "医疗保障局", "商务局",
    "文化广电旅游体育局", "生态环境局", "政务服务和数据管理局",
    "城市管理和综合执法局", "退役军人事务局", "宣传部",
    "司法局", "住房和建设局", "建筑工务署", "民政局", "财政局",
    "气象局", "应急管理局", "审计局", "政府办公厅", "统计局", "信访局",
]

THRESHOLD = 15  # 达标线


def count_files(city_dir):
    """统计城市目录中的有效文件数"""
    if not city_dir.exists():
        return 0
    count = 0
    for f in city_dir.iterdir():
        if f.suffix in ('.pdf', '.html') and f.name != '.gitkeep':
            if f.stat().st_size > 500:
                count += 1
    return count


def identify_depts_from_files(city_dir):
    """从文件名推断已覆盖的部门"""
    found = set()
    if not city_dir.exists():
        return found
    for f in city_dir.iterdir():
        name = f.stem
        for dept in TARGET_DEPTS:
            if dept.replace("局", "").replace("委员会", "") in name:
                found.add(dept)
                break
    return found


def generate_report():
    os.makedirs(REPORT_FILE.parent, exist_ok=True)

    with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
        cities = json.load(f)

    results = []
    total_files = 0
    pass_count = 0
    weak_cities = []
    zero_cities = []

    for c in cities:
        rank = c['rank']
        city = c['city']
        city_key = f"{rank:03d}_{city}"
        city_dir = BASE_DIR / city_key
        n = count_files(city_dir)
        total_files += n
        passed = n >= THRESHOLD
        if passed:
            pass_count += 1
        elif n == 0:
            zero_cities.append((city_key, n))
        else:
            weak_cities.append((city_key, n))
        results.append((city_key, n, passed))

    # 按文件数排序(弱城市)
    weak_cities.sort(key=lambda x: x[1])

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    lines = []
    lines.append(f"# 预算爬取进度报告")
    lines.append(f"生成时间: {now}")
    lines.append(f"")
    lines.append(f"## 总览")
    lines.append(f"- 达标城市 (>={THRESHOLD}个文件): {pass_count}/100")
    lines.append(f"- 弱城市 (1~{THRESHOLD-1}个文件): {len(weak_cities)}")
    lines.append(f"- 零文件城市: {len(zero_cities)}")
    lines.append(f"- 总文件数: {total_files}")
    lines.append(f"")

    if zero_cities:
        lines.append(f"## 零文件城市 ({len(zero_cities)})")
        for ck, n in zero_cities:
            lines.append(f"  {ck}: {n}")
        lines.append("")

    if weak_cities:
        lines.append(f"## 弱城市 (1~{THRESHOLD-1}个文件, 共{len(weak_cities)})")
        for ck, n in weak_cities:
            lines.append(f"  {ck}: {n}")
        lines.append("")

    lines.append(f"## 全部城市文件数")
    for ck, n, passed in results:
        mark = "✓" if passed else "✗"
        lines.append(f"  {mark} {ck}: {n}")

    report = "\n".join(lines)

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report)

    # 也写STATUS.md
    status_lines = [
        f"# 预算爬取状态",
        f"更新: {now}",
        f"",
        f"达标: {pass_count}/100 城市 (>={THRESHOLD}个部门文件)",
        f"弱: {len(weak_cities)} | 零: {len(zero_cities)} | 总文件: {total_files}",
    ]
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(status_lines))

    print(report)
    return pass_count, weak_cities, zero_cities


if __name__ == '__main__':
    generate_report()
