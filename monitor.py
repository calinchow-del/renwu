#!/usr/bin/env python3
"""
监控+调优脚本
功能:
  1. 生成进度报告
  2. 识别弱城市(覆盖率低)
  3. 输出调优建议
  4. 自动触发重试
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent / "预算数据"
PROGRESS_V12 = Path(__file__).parent / "scrape_progress_v12.json"
PROGRESS_V11 = Path(__file__).parent / "scrape_progress.json"
CITY_DATA_FILE = Path(__file__).parent / "city_data.json"
REPORT_FILE = Path(__file__).parent / "logs" / "monitor_report.txt"

TARGET_DEPTS = [
    "卫生健康委员会", "教育局", "发展和改革局", "规划和自然资源局",
    "交通运输局", "科技创新局", "水务局", "人力资源和社会保障局",
    "工业和信息化局", "市场监督管理局", "国有资产监督管理委员会",
    "公安局", "公安局交通警察局", "医疗保障局", "商务局",
    "文化广电旅游体育局", "生态环境局", "政务服务和数据管理局",
    "城市管理和综合执法局", "退役军人事务局", "宣传部",
    "司法局", "住房和建设局", "建筑工务署", "民政局", "财政局",
    "气象局", "应急管理局", "审计局", "政府办公厅", "统计局", "信访局",
    "口岸办公室",
]

MIN_COVERAGE = 15


def count_files(city_key):
    folder = BASE_DIR / city_key
    if not folder.exists():
        return 0, 0
    pdfs = 0
    for f in folder.iterdir():
        if f.name in ('.gitkeep', '爬取汇总.txt'):
            continue
        if f.stat().st_size < 500:
            continue
        if f.suffix == '.pdf':
            pdfs += 1
    return pdfs, 0


def load_progress():
    """合并v11和v12进度"""
    merged = {}
    for pf in [PROGRESS_V11, PROGRESS_V12]:
        if pf.exists():
            try:
                with open(pf, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for k, v in data.get('completed', {}).items():
                    if isinstance(v, dict):
                        existing = merged.get(k, {})
                        if v.get('found', 0) > existing.get('found', 0):
                            merged[k] = v
            except:
                pass
    return merged


def generate_report():
    with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
        cities = json.load(f)

    progress = load_progress()

    report = []
    report.append(f"=" * 70)
    report.append(f"预算爬取监控报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"=" * 70)

    # 统计
    excellent = []   # >=25
    good = []        # >=15
    weak = []        # 1-14
    empty = []       # 0
    not_attempted = []

    total_pdfs = 0
    total_htmls = 0

    for c in cities:
        ck = f"{c['rank']:03d}_{c['city']}"
        pdfs, htmls = count_files(ck)
        total_pdfs += pdfs
        total_htmls += htmls

        prev = progress.get(ck, {})
        found = prev.get('found', 0) if isinstance(prev, dict) else 0
        # 也用磁盘文件数作为参考
        disk_count = pdfs + htmls
        effective = max(found, disk_count)

        entry = {
            'rank': c['rank'],
            'city': c['city'],
            'found': found,
            'disk_pdfs': pdfs,
            'disk_htmls': htmls,
            'effective': effective,
            'strategy': prev.get('strategy', 'unknown') if isinstance(prev, dict) else 'unknown',
            'depts': prev.get('depts', []) if isinstance(prev, dict) else [],
        }

        if effective == 0:
            if ck in progress:
                empty.append(entry)
            else:
                not_attempted.append(entry)
        elif effective < MIN_COVERAGE:
            weak.append(entry)
        elif effective >= 25:
            excellent.append(entry)
        else:
            good.append(entry)

    report.append(f"\n总计: {len(cities)} 城市")
    report.append(f"  优秀(>=25部门): {len(excellent)}")
    report.append(f"  合格(>=15部门): {len(good)}")
    report.append(f"  不足(<15部门):  {len(weak)}")
    report.append(f"  空白(0):        {len(empty)}")
    report.append(f"  未尝试:         {len(not_attempted)}")
    report.append(f"  磁盘PDF总数:    {total_pdfs}")
    report.append(f"  磁盘HTML总数:   {total_htmls}")

    # 需要行动的城市
    action_needed = weak + empty + not_attempted
    action_needed.sort(key=lambda x: x['rank'])

    if action_needed:
        report.append(f"\n{'='*70}")
        report.append(f"需要行动的城市 ({len(action_needed)}个):")
        report.append(f"{'='*70}")
        for e in action_needed:
            status = "未尝试" if e in not_attempted else f"找到{e['effective']}个"
            report.append(f"  [{e['rank']:3d}] {e['city']:6s} - {status} (PDF:{e['disk_pdfs']} HTML:{e['disk_htmls']}) 策略:{e['strategy']}")

    # 优秀城市列表
    if excellent:
        report.append(f"\n优秀城市 ({len(excellent)}个):")
        for e in sorted(excellent, key=lambda x: x['effective'], reverse=True)[:10]:
            report.append(f"  [{e['rank']:3d}] {e['city']:6s} - {e['effective']}个部门 (PDF:{e['disk_pdfs']})")

    # 部门覆盖热力图 - 哪些部门最难找
    dept_found_count = {d: 0 for d in TARGET_DEPTS}
    for ck, v in progress.items():
        if isinstance(v, dict) and 'depts' in v:
            for d in v['depts']:
                if d in dept_found_count:
                    dept_found_count[d] += 1

    report.append(f"\n{'='*70}")
    report.append(f"部门覆盖率 (已尝试的城市中):")
    report.append(f"{'='*70}")
    attempted = len(progress)
    for dept, count in sorted(dept_found_count.items(), key=lambda x: x[1]):
        pct = count / max(attempted, 1) * 100
        bar = "█" * int(pct / 5)
        report.append(f"  {dept:20s} {count:3d}/{attempted:3d} ({pct:5.1f}%) {bar}")

    # 调优建议
    report.append(f"\n{'='*70}")
    report.append(f"调优建议:")
    report.append(f"{'='*70}")

    if not_attempted:
        report.append(f"  1. 有 {len(not_attempted)} 个城市未尝试, 建议先全量跑一轮")
    if weak:
        report.append(f"  2. 有 {len(weak)} 个城市覆盖率低, 建议用 --retry-weak --force 重试")
    if empty:
        report.append(f"  3. 有 {len(empty)} 个城市为空, 可能需要人工检查URL或网站结构")

    # 最难找的5个部门
    hard_depts = sorted(dept_found_count.items(), key=lambda x: x[1])[:5]
    if hard_depts and attempted > 0:
        report.append(f"  4. 最难覆盖的部门: {', '.join(d[0] for d in hard_depts)}")

    report_text = "\n".join(report)

    # 写文件
    os.makedirs(REPORT_FILE.parent, exist_ok=True)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        f.write(report_text)

    # 也输出到stdout
    print(report_text)

    # 返回行动建议码
    if not_attempted:
        return "RUN_FULL"        # 需要全量运行
    elif weak or empty:
        return "RETRY_WEAK"      # 需要重试弱城市
    else:
        return "ALL_GOOD"        # 全部达标


if __name__ == '__main__':
    action = generate_report()
    print(f"\n建议操作: {action}")
    sys.exit(0 if action == "ALL_GOOD" else 1)
