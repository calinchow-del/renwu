#!/usr/bin/env python3
"""
PDF质量校验 + 重命名 + 清理
最终文件名格式: 2026年XX市XX局部门预算.pdf
从PDF标题中提取真实部门名称
"""

import json
import os
import re
import sys
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    print("WARNING: PyMuPDF未安装, PDF校验将跳过")
    fitz = None

BASE_DIR = Path(__file__).parent / "预算数据"
CITY_DATA = Path(__file__).parent / "city_data.json"

# 加载城市名称映射: "005_广州" -> "广州"
def load_city_map():
    cities = {}
    try:
        with open(CITY_DATA, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for c in data:
            key = f"{c['rank']:03d}_{c['city']}"
            cities[key] = c['city']
    except:
        pass
    return cities

# 目标部门关键词 -> 用于判断是否为我们要的部门（宽松匹配）
DEPT_KEYWORDS = [
    (["公安局交通警察", "公安交通管理", "交警局", "交通警察局", "交警支队", "公安局交管", "公安交警", "交通管理局"], "公安局交通警察局"),
    (["卫生健康委", "卫健委", "卫生健康局", "卫生局"], "卫生健康委员会"),
    (["教育局", "教育委员会", "教育委", "教委", "教育体育局", "教体局"], "教育局"),
    (["发展和改革", "发展改革", "发改委", "发改局"], "发展和改革局"),
    (["规划和自然资源", "自然资源和规划", "自然资源局", "规划局", "国土资源"], "规划和自然资源局"),
    (["交通运输局", "交通运输委", "交通局", "交通委"], "交通运输局"),
    (["科技创新局", "科学技术局", "科技局", "科技创新委"], "科技创新局"),
    (["水务局", "水利局", "水利水务"], "水务局"),
    (["人力资源和社会保障", "人力资源社会保障", "人社局", "人力社保局"], "人力资源和社会保障局"),
    (["工业和信息化", "工信局", "经济和信息化", "经信局", "经信委"], "工业和信息化局"),
    (["市场监督管理", "市场监管局", "市场监管"], "市场监督管理局"),
    (["国有资产监督管理", "国资委"], "国有资产监督管理委员会"),
    (["口岸办公室", "口岸办", "口岸事务", "口岸局", "口岸管理"], "口岸办公室"),
    (["医疗保障局", "医保局"], "医疗保障局"),
    (["商务局", "商务委"], "商务局"),
    (["文化广电旅游体育", "文化广电旅游", "文化和旅游", "文旅局", "文广旅体", "文化体育旅游", "文化广电和旅游", "文体广旅", "文体广电旅游", "文广新旅", "文体旅游", "文化旅游"], "文化广电旅游体育局"),
    (["生态环境局", "环境保护局", "环保局"], "生态环境局"),
    (["政务服务和数据管理", "政务服务数据管理", "政务服务局", "大数据管理局", "大数据发展局", "数据局", "数据管理局", "行政审批局", "行政审批服务局", "政务和大数据局", "政数局"], "政务服务和数据管理局"),
    (["城市管理和综合执法", "城市管理综合执法", "城市管理局", "城管局", "城管执法", "综合行政执法", "综合执法局", "城市管理委员会", "城管和综合执法"], "城市管理和综合执法局"),
    (["退役军人事务", "退役军人局"], "退役军人事务局"),
    (["宣传部", "市委宣传部", "市委宣传", "委宣传部"], "宣传部"),
    (["司法局"], "司法局"),
    (["住房和建设", "住房和城乡建设", "住建局", "住房建设", "住房城乡建设", "住建委", "住房保障和房屋管理"], "住房和建设局"),
    (["建筑工务署", "建筑工务中心", "建设工程事务", "建筑工务", "工务署"], "建筑工务署"),
    (["民政局"], "民政局"),
    (["财政局"], "财政局"),
    (["气象局", "气象台", "气象部门", "气象服务"], "气象局"),
    (["应急管理局", "应急局", "安全生产监督"], "应急管理局"),
    (["审计局"], "审计局"),
    (["政府办公厅", "政府办公室", "人民政府办公"], "政府办公厅"),
    (["统计局"], "统计局"),
    (["信访局", "信访办公室", "信访办"], "信访局"),
    # 公安局要放在交通警察局后面，避免误匹配
    (["公安局", "市公安局"], "公安局"),
]

# 子单位关键词
SUB_UNIT_KEYWORDS = [
    "中心", "研究院", "研究所", "学校", "学院", "医院", "站",
    "大队", "支队", "总队", "干部", "老干", "活动室",
    "服务中心", "事务中心", "管理所", "监测", "培训", "幼儿园",
    "纪念馆", "博物馆", "图书馆", "文化馆", "美术馆",
    "福利院", "救助", "戒毒", "疗养", "供应站",
    "分局", "监管局", "管理局",
]

# 区域分局模式（如"深汕监管局"、"大鹏管理局"）
SUB_REGION_PATTERN = re.compile(
    r'(深汕|前海|南山|福田|罗湖|龙岗|龙华|坪山|光明|盐田|宝安|大鹏|'
    r'天河|越秀|海珠|白云|黄埔|花都|番禺|南沙|从化|增城|'
    r'浦东|虹口|静安|黄浦|徐汇|长宁|普陀|闵行|宝山|嘉定|松江|青浦|奉贤|崇明|'
    r'朝阳|海淀|丰台|石景山|通州|顺义|昌平|大兴|'
    r'[东西南北]城)'
    r'(分局|监管局|管理局|管理处|监管处|执法局|支队)'
)


def extract_pdf_title(filepath):
    """用PyMuPDF提取PDF第一页标题"""
    if not fitz:
        return None
    try:
        doc = fitz.open(str(filepath))
        if len(doc) == 0:
            doc.close()
            return None
        page = doc[0]

        # 提取带字号的文本块，取最大字号的作为标题
        blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
        title_candidates = []
        for block in blocks:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    text = span["text"].strip()
                    size = span["size"]
                    if text and len(text) > 2:
                        title_candidates.append((size, text))

        doc.close()

        if not title_candidates:
            return None

        # 按字号排序，取最大的
        title_candidates.sort(key=lambda x: x[0], reverse=True)
        max_size = title_candidates[0][0]
        # 取字号相近的文本拼接
        title_parts = [t for s, t in title_candidates if s >= max_size - 1]
        title = "".join(title_parts[:3])

        return title.strip()
    except:
        return None


def match_dept_from_title(title):
    """从标题匹配目标部门，返回(标准部门名, matched_keyword)"""
    if not title:
        return None
    for keywords, std_dept in DEPT_KEYWORDS:
        for kw in keywords:
            if kw in title:
                return std_dept
    return None


def extract_real_dept_name(title, city_name):
    """从PDF标题中提取真实的部门全称
    例如: "2026年广州市发展和改革委员会部门预算" -> "发展和改革委员会"
    """
    if not title:
        return None

    # 清理标题
    t = title.strip()
    t = re.sub(r'\s+', '', t)

    # 尝试匹配: "2026年XX市YYY部门预算" 格式
    patterns = [
        # 2026年XX市YYY部门预算
        rf'2026年度?{re.escape(city_name)}市(.+?)部门预算',
        # XX市YYY2026年部门预算
        rf'{re.escape(city_name)}市(.+?)2026年度?部门预算',
        # 2026年YYY部门预算 (无城市名)
        r'2026年度?(.+?)部门预算',
        # YYY2026年部门预算
        r'(.+?)2026年度?部门预算',
        # YYY部门预算
        r'(.+?)部门预算',
    ]

    for pat in patterns:
        m = re.search(pat, t)
        if m:
            dept = m.group(1).strip()
            # 清理前缀: "中共XX市委员会" / "中国共产党XX市委员会" -> 保留后面的部门名
            dept = re.sub(r'^中国共产党' + re.escape(city_name) + r'市委员会', '', dept)
            dept = re.sub(r'^中共' + re.escape(city_name) + r'市委', '', dept)
            dept = re.sub(r'^' + re.escape(city_name) + r'市', '', dept)
            # 人民政府下属部门保留"人民政府"前缀（如"人民政府办公厅"）
            if dept and len(dept) > 1:
                return dept

    return None


def is_sub_unit(title, real_dept_name):
    """判断是否为子单位"""
    if not title:
        return False
    # "本级"明确表示是主体
    if "本级" in title:
        return False
    if not real_dept_name:
        return False

    # 检查区域分局模式
    if SUB_REGION_PATTERN.search(real_dept_name or '') or SUB_REGION_PATTERN.search(title):
        return True

    # 如果部门名称本身就包含子单位关键词（如"服务中心"），要小心判断
    for sub_kw in SUB_UNIT_KEYWORDS:
        if sub_kw in title and sub_kw not in real_dept_name:
            return True

    return False


def safe_filename(s):
    """清理文件名中的非法字符"""
    return re.sub(r'[\\/:*?"<>|]', '', s).strip()


def validate_city(city_dir, city_name):
    """校验并重命名单个城市目录下的PDF"""
    stats = {"renamed": 0, "deleted": 0, "kept": 0}

    # 收集所有PDF
    files = sorted([f for f in city_dir.iterdir()
                    if f.suffix == '.pdf' and f.name != '.gitkeep'])

    # 用于去重：标准部门名 -> (文件路径, 文件大小, 新文件名)
    dept_best = {}

    for f in files:
        if f.stat().st_size < 500:
            print(f"  DELETE (太小): {city_dir.name}/{f.name}")
            f.unlink()
            stats["deleted"] += 1
            continue

        # 提取标题
        title = extract_pdf_title(f)

        if not title:
            # 无法读取标题，尝试从文件名推断
            if f.name.startswith("2026年") and "部门预算" in f.name:
                stats["kept"] += 1
                continue
            print(f"  DELETE (无法读取标题): {city_dir.name}/{f.name}")
            f.unlink()
            stats["deleted"] += 1
            continue

        # 如果标题是通用的（如上海"上海市2026年市级单位预算"），从文件名提取部门
        if title and ("市级单位预算" in title or "单位预算" in title) and "部门预算" not in title:
            # 尝试从文件名匹配部门
            fn_dept = match_dept_from_title(f.name)
            if fn_dept:
                # 从文件名提取真实部门名
                fn_match = re.search(r'([^_/]+?)(?:2026|部门预算|\.\w+$)', f.name)
                real_dept_from_fn = fn_match.group(1).strip('_') if fn_match else None
                if real_dept_from_fn and "部门预算" in f.name:
                    # 文件名已经是正确格式
                    std_dept = fn_dept
                    title = f.name  # 用文件名作为标题来继续处理
                else:
                    # 这是"单位预算"而非"部门预算"，可能是子单位
                    print(f"  DELETE (单位预算非部门预算): {city_dir.name}/{f.name} | 标题: {title[:60]}")
                    f.unlink()
                    stats["deleted"] += 1
                    continue
            else:
                print(f"  DELETE (通用标题无法匹配): {city_dir.name}/{f.name} | 标题: {title[:60]}")
                f.unlink()
                stats["deleted"] += 1
                continue

        # 匹配目标部门
        std_dept = match_dept_from_title(title)

        if not std_dept:
            if "预算" not in title:
                print(f"  DELETE (非预算): {city_dir.name}/{f.name} | 标题: {title[:60]}")
            else:
                print(f"  DELETE (非目标部门): {city_dir.name}/{f.name} | 标题: {title[:60]}")
            f.unlink()
            stats["deleted"] += 1
            continue

        # 提取真实部门名
        real_dept = extract_real_dept_name(title, city_name)

        # 检查子单位
        if is_sub_unit(title, real_dept or std_dept):
            print(f"  DELETE (子单位): {city_dir.name}/{f.name} | 标题: {title[:60]}")
            f.unlink()
            stats["deleted"] += 1
            continue

        # 构造新文件名: 2026年XX市YYY部门预算.pdf
        if real_dept:
            dept_display = real_dept
        else:
            dept_display = std_dept

        # 清理部门名中的多余内容
        # 去掉"（本级）"等括号内容
        dept_clean = re.sub(r'[（(][^）)]*[）)]', '', dept_display).strip()
        # 去掉年份（如残留的"2026"）
        dept_clean = re.sub(r'2026年?度?', '', dept_clean).strip()
        # 去掉城市名前缀（可能残留）
        dept_clean = re.sub(r'^' + re.escape(city_name) + r'市?', '', dept_clean).strip()
        # 去掉"委员会宣传部" -> "宣传部"（中共XX市委员会宣传部）
        dept_clean = re.sub(r'^委员会', '', dept_clean).strip()
        # 去掉"部门"后缀（避免"部门部门预算"）
        dept_clean = re.sub(r'部门$', '', dept_clean).strip()
        # 去掉"年度"等多余文字
        dept_clean = re.sub(r'年度$', '', dept_clean).strip()
        # 去掉"国家"前缀（如"国家信访局"应为"信访局"，但保留"国有资产"）
        if not dept_clean.startswith("国有"):
            dept_clean = re.sub(r'^国家', '', dept_clean).strip()

        if not dept_clean or len(dept_clean) < 2:
            dept_clean = std_dept

        new_name = f"2026年{city_name}市{safe_filename(dept_clean)}部门预算.pdf"

        # 去重：同一标准部门只保留最大的文件
        fsize = f.stat().st_size
        if std_dept in dept_best:
            old_f, old_size, old_new_name = dept_best[std_dept]
            if fsize > old_size:
                # 新文件更大，删除旧的
                if old_f.exists():
                    print(f"  DELETE (重复较小): {city_dir.name}/{old_f.name}")
                    old_f.unlink()
                    stats["deleted"] += 1
                dept_best[std_dept] = (f, fsize, new_name)
            else:
                # 旧文件更大，删除新的
                print(f"  DELETE (重复较小): {city_dir.name}/{f.name}")
                f.unlink()
                stats["deleted"] += 1
                continue
        else:
            dept_best[std_dept] = (f, fsize, new_name)

    # 执行重命名
    for std_dept, (f, fsize, new_name) in dept_best.items():
        if not f.exists():
            continue
        new_path = f.parent / new_name
        if new_path == f:
            stats["kept"] += 1
            continue
        # 如果目标文件已存在（不应该，但以防万一）
        if new_path.exists() and new_path != f:
            if new_path.stat().st_size >= fsize:
                print(f"  DELETE (已有更大): {city_dir.name}/{f.name}")
                f.unlink()
                stats["deleted"] += 1
                continue
            else:
                new_path.unlink()
        print(f"  RENAME: {city_dir.name}/{f.name} -> {new_name}")
        f.rename(new_path)
        stats["renamed"] += 1

    # 删除爬取汇总.txt（不需要了）
    summary = city_dir / "爬取汇总.txt"
    if summary.exists():
        summary.unlink()

    return stats


def main():
    if not BASE_DIR.exists():
        print(f"ERROR: 预算数据目录不存在: {BASE_DIR}")
        return

    city_map = load_city_map()
    total_stats = {"renamed": 0, "deleted": 0, "kept": 0}

    city_dirs = sorted([d for d in BASE_DIR.iterdir() if d.is_dir()])
    print(f"校验 {len(city_dirs)} 个城市目录...")
    print("=" * 70)

    for city_dir in city_dirs:
        city_name = city_map.get(city_dir.name, city_dir.name.split('_', 1)[-1])
        stats = validate_city(city_dir, city_name)
        for k in total_stats:
            total_stats[k] += stats[k]

    print("=" * 70)
    print(f"校验完成:")
    print(f"  重命名: {total_stats['renamed']}")
    print(f"  删除:   {total_stats['deleted']}")
    print(f"  保留:   {total_stats['kept']}")

    # 显示最终文件列表示例
    print("\n" + "=" * 70)
    print("示例城市文件列表:")
    for city_dir in city_dirs[:3]:
        pdfs = sorted([f.name for f in city_dir.iterdir() if f.suffix == '.pdf'])
        if pdfs:
            print(f"\n[{city_dir.name}]")
            for p in pdfs:
                print(f"  {p}")


if __name__ == '__main__':
    main()
