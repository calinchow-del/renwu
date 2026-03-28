#!/usr/bin/env python3
"""
中国百强城市2026年部门预算爬取脚本 v12
修复: SSL回退、分页检测、部门匹配增强、--retry-weak/--force支持
"""

import json
import os
import re
import time
import logging
import urllib3
import traceback
from urllib.parse import urljoin, urlparse, unquote, quote
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
from bs4 import BeautifulSoup

# ========== 配置 ==========
BASE_DIR = Path(__file__).parent / "预算数据"
LOG_FILE = Path(__file__).parent / "logs" / "scrape.log"
PROGRESS_FILE = Path(__file__).parent / "scrape_progress.json"
CITY_DATA_FILE = Path(__file__).parent / "city_data.json"

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
}
TIMEOUT = 15
RETRY = 2
MAX_WORKERS = 1  # v10: 顺序处理,不用多线程
MAX_PROBE_PATHS = 15  # v11: 最多探测15个路径就停止
DELAY_PAGE = 0.5
DELAY_CITY = 2

# ========== 32个目标委办局 ==========
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

MATCH_RULES = [
    (["公安局交通警察", "公安交通管理", "交警局", "交通警察局", "交警支队"], "公安局交通警察局"),
    (["卫生健康委", "卫健委", "卫生健康局", "卫生局"], "卫生健康委员会"),
    (["教育局", "教育委员会", "教育委"], "教育局"),
    (["发展和改革", "发展改革", "发改委", "发改局"], "发展和改革局"),
    (["规划和自然资源", "自然资源和规划", "自然资源局", "规划局", "国土资源"], "规划和自然资源局"),
    (["交通运输局", "交通运输委", "交通局", "交通委"], "交通运输局"),
    (["科技创新局", "科学技术局", "科技局", "科技创新委"], "科技创新局"),
    (["水务局", "水利局", "水利水务"], "水务局"),
    (["人力资源和社会保障", "人力资源社会保障", "人社局"], "人力资源和社会保障局"),
    (["工业和信息化", "工信局", "经济和信息化", "经信局", "经信委"], "工业和信息化局"),
    (["市场监督管理", "市场监管"], "市场监督管理局"),
    (["国有资产监督管理", "国资委"], "国有资产监督管理委员会"),
    (["公安局", "市公安局"], "公安局"),
    (["医疗保障局", "医保局"], "医疗保障局"),
    (["商务局", "商务委"], "商务局"),
    (["文化广电旅游体育", "文化广电旅游", "文化和旅游", "文旅局", "文广旅体", "文化体育旅游", "文化旅游广电", "文广旅局", "文广新旅"], "文化广电旅游体育局"),
    (["生态环境局", "环境保护局", "环保局", "生态环境厅"], "生态环境局"),
    (["政务服务和数据管理", "政务服务数据管理", "政务服务局", "大数据管理局", "数据局", "行政审批局", "大数据发展", "政务服务中心", "政数局"], "政务服务和数据管理局"),
    (["城市管理和综合执法", "城市管理综合执法", "城市管理局", "城管局", "城管执法", "综合行政执法", "城市管理委员会"], "城市管理和综合执法局"),
    (["退役军人事务", "退役军人局"], "退役军人事务局"),
    (["宣传部", "市委宣传部"], "宣传部"),
    (["司法局"], "司法局"),
    (["住房和建设", "住房和城乡建设", "住建局", "住房建设", "住房城乡建设", "住房保障和房屋管理"], "住房和建设局"),
    (["建筑工务署", "建筑工务中心", "建设工程事务", "建设工务"], "建筑工务署"),
    (["民政局"], "民政局"),
    (["财政局"], "财政局"),
    (["气象局", "气象台"], "气象局"),
    (["应急管理局", "应急局", "安全生产监督", "应急管理委"], "应急管理局"),
    (["审计局"], "审计局"),
    (["政府办公厅", "政府办公室", "人民政府办公", "市政府办公"], "政府办公厅"),
    (["统计局"], "统计局"),
    (["信访局", "信访办", "群众来访接待"], "信访局"),
]

# ========== 日志 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== 进度管理(v10: 无锁,顺序处理) ==========

def load_progress():
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"completed": {}, "failed": []}
        if not isinstance(data, dict):
            return {"completed": {}, "failed": []}
        if not isinstance(data.get('completed'), dict):
            data['completed'] = {}
        if not isinstance(data.get('failed'), list):
            data['failed'] = []
        return data
    return {"completed": {}, "failed": []}

def save_progress(progress_data):
    try:
        safe_data = {
            "completed": dict(progress_data.get("completed", {})),
            "failed": list(progress_data.get("failed", []))
        }
        tmp = str(PROGRESS_FILE) + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(safe_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, str(PROGRESS_FILE))
    except Exception as e:
        logger.error(f"保存进度失败: {e}")

def update_city_progress(progress_data, city_key, found, downloaded):
    if 'completed' not in progress_data:
        progress_data['completed'] = {}
    progress_data['completed'][city_key] = {
        "found": found,
        "downloaded": downloaded,
        "time": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    save_progress(progress_data)


def count_city_files(city_key):
    """检查磁盘上该城市已有多少个预算文件(PDF/HTML)"""
    city_folder = BASE_DIR / city_key
    if not city_folder.exists():
        return 0
    count = 0
    try:
        for f in city_folder.iterdir():
            if f.suffix in ('.pdf', '.html') and f.name != '.gitkeep':
                if f.stat().st_size > 500:
                    count += 1
    except Exception:
        pass
    return count

# ========== 核心工具 ==========

def fetch(session, url, timeout=TIMEOUT):
    for i in range(RETRY):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True, verify=False)
            r.encoding = r.apparent_encoding or 'utf-8'
            if r.status_code == 200:
                return r
        except requests.exceptions.SSLError:
            # SSL失败 → 尝试http回退
            if url.startswith('https://'):
                http_url = url.replace('https://', 'http://', 1)
                try:
                    r = session.get(http_url, timeout=timeout, allow_redirects=True, verify=False)
                    r.encoding = r.apparent_encoding or 'utf-8'
                    if r.status_code == 200:
                        return r
                except Exception:
                    pass
        except Exception:
            if i < RETRY - 1:
                time.sleep(1)
    return None

def match_dept(text, city=""):
    if not text or len(text.strip()) < 2:
        return None
    t = text.strip()
    for p in [city + "市", city, "市"]:
        if p and t.startswith(p):
            t = t[len(p):]
    for keywords, dept in MATCH_RULES:
        for kw in keywords:
            if kw in t:
                if dept == "公安局" and ("交通" in t or "交警" in t):
                    return "公安局交通警察局"
                return dept
    return None

def is_main_dept_budget(text, dept_name, city):
    t = text.strip()
    if "预算" not in t:
        return False, 0
    score = 0
    if "部门预算" in t:
        score += 100
    for keywords, dept in MATCH_RULES:
        if dept == dept_name:
            for kw in keywords:
                if kw in t:
                    score += 50
                    break
            break
    if "2026" in t:
        score += 20
    sub_unit_keywords = [
        "中心", "研究院", "研究所", "学校", "学院", "医院", "站",
        "大队", "支队", "总队", "干部", "老干", "活动室", "活动中心",
        "服务中心", "事务中心", "管理所", "监测", "培训", "幼儿园",
        "纪念馆", "博物馆", "图书馆", "文化馆", "美术馆", "展示馆",
        "福利院", "救助", "戒毒", "疗养", "供应站", "试验场",
        "分局", "管理局", "监管局",
        "本级", "本部",
    ]
    if "部门预算" not in t:
        for kw in sub_unit_keywords:
            if kw in t:
                score -= 80
                break
    return score > 0, score

def download_pdf(session, url, path):
    urls_to_try = [url]
    if url.startswith('https://'):
        urls_to_try.append(url.replace('https://', 'http://', 1))
    for u in urls_to_try:
        try:
            r = session.get(u, timeout=60, stream=True, verify=False)
            if r.status_code == 200:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                if os.path.getsize(path) > 1000:
                    return True
                else:
                    os.remove(path)
        except Exception:
            pass
    return False

def safe_filename(s, maxlen=80):
    return re.sub(r'[\\/:*?"<>|\s]', '_', s)[:maxlen]

# ========== 页面解析 ==========

def extract_all_links(session, url, city):
    r = fetch(session, url)
    if not r:
        return [], ""
    try:
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return [], r.text
    results = []
    seen_urls = set()
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href'].strip()
        if not text or not href or href.startswith('#') or href.startswith('javascript'):
            continue
        full = urljoin(url, href)
        if full in seen_urls:
            continue
        seen_urls.add(full)
        dept = match_dept(text, city)
        results.append((text, full, dept))
    return results, r.text

def detect_pagination_urls(base_url, html):
    """检测分页URL模式"""
    pages = []
    parsed = urlparse(base_url)

    # 模式1: index_N.html / index_N.shtml
    for ext in ['.html', '.shtml', '.htm']:
        m = re.search(r'(index(?:_\d+)?)' + re.escape(ext), base_url)
        if m:
            prefix = base_url[:base_url.rfind(m.group(0))]
            for i in range(2, 8):
                pages.append(f"{prefix}index_{i}{ext}")
            break

    # 模式2: column-index-N.shtml
    m = re.search(r'column-index-(\d+)\.shtml', base_url)
    if m:
        prefix = base_url[:base_url.rfind(m.group(0))]
        for i in range(2, 8):
            pages.append(f"{prefix}column-index-{i}.shtml")

    # 模式3: /N.html (如 /1.html, /2.html)
    m = re.search(r'/(\d+)\.(html|shtml)$', base_url)
    if m:
        prefix = base_url[:base_url.rfind(m.group(0)) + 1]
        start = int(m.group(1))
        for i in range(start + 1, start + 7):
            pages.append(f"{prefix}{i}.{m.group(2)}")

    # 模式4: ?page=N 或 &page=N (从HTML中提取)
    if html:
        page_links = re.findall(r'href=["\']([^"\']*(?:page|pageNum|pageIndex|p)=\d+[^"\']*)["\']', html)
        for pl in page_links[:10]:
            full = urljoin(base_url, pl)
            if full not in pages:
                pages.append(full)

    return pages[:10]


def find_dept_budgets(session, budget_url, city):
    found = {}
    visited = set()

    def scan_page(url, depth=0):
        if url in visited or depth > 2:
            return
        visited.add(url)
        try:
            links, html = extract_all_links(session, url, city)
        except Exception as e:
            logger.warning(f"[{city}] 扫描页面异常 {url}: {e}")
            return
        if not links:
            return
        sub_pages = []
        for text, link_url, dept in links:
            if dept:
                is_pdf = link_url.lower().endswith('.pdf')
                is_main, score = is_main_dept_budget(text, dept, city)
                if dept not in found:
                    found[dept] = []
                if not any(u == link_url for _, u, _, _ in found[dept]):
                    found[dept].append((text, link_url, is_pdf, score))
            else:
                if depth < 2 and any(kw in text for kw in ['部门预算', '预算公开', '2026', '下一页', '更多', '下页']):
                    sub_pages.append(link_url)

        # 自动检测分页URL
        if depth == 0 and html:
            pagination_urls = detect_pagination_urls(url, html)
            for pu in pagination_urls:
                if pu not in visited:
                    sub_pages.append(pu)

        for sub_url in sub_pages[:15]:
            time.sleep(DELAY_PAGE)
            scan_page(sub_url, depth + 1)

    scan_page(budget_url)
    return found

# ========== 预算路径探测 ==========

COMMON_BUDGET_PATHS = [
    # 最常见的路径优先
    "/zwgk/zdly/czxx/bmczyjs/",
    "/zwgk/zdly/czzj/bmyjshsgjf/ys/",
    "/zwgk/zdly/czzj/bmczyjs/2026/",
    "/zwgk/zdly/czzj/bmczyjs/",
    "/zwgk/zdly/czxx/",
    "/zwgk/zdly/czzj/",
    "/zwgk/czzj/", "/zwgk/bmys/", "/zwgk/czgk/",
    "/zfxxgk/fdzdgknr/czxx/",
    "/zfxxgk/fdzdgknr/czxx/bmczyjs/",
    "/zfxxgk/fdzdgknr/czxx/bmczyjs/2026/",
    "/zfxxgk/fdzdgknr/ysjs/",
    "/zfxxgk/fdzdgknr/ysjs/bmczyjsbgjsgjf/",
    "/zfxxgk/czxx/", "/zfxxgk/bmczyjs/",
    "/xxgk/czxx/", "/xxgk/fdzdgk/czxx/",
    "/xxgk/fdzdgk/czxx/bmczyjs/",
    "/szf/ztzl/ysgk/", "/yjsgk/",
    "/zwgk/fdzdgknr/ysjs/bmczyjsbgjsgjf/",
    "/site/tpl/szfbmczyjs/", "/gkml/czyjs/",
    "/zwgk/zfxxgkzl/fdzdgknr/ysjs/",
    "/zwgk/zfxxgkzl/fdzdgknr/czxx/",
    # 额外路径
    "/zwgk/zdly/czxx/czyjs/",
    "/zfxxgk/fdzdgknr/bmczyjs/",
    "/zfxxgk/fdzdgknr/bmczyjs/2026/",
    "/zwgk/czxx/bmczyjs/",
    "/zwgk/czxx/bmczyjs/2026/",
    "/czyjs/", "/bmys/", "/czys/",
    "/zfxxgk/zdlyxxgk/czyjshsg/bmyjs/",
    "/ncszf/2026bmys/",
    "/zwgk/zfxxgk/czxx/",
    "/ztzl/yjsgk/", "/ztzl/ysgk/",
    "/zfb/xxgk/ztxxgk/czzj/bmczyjs/",
    "/col/col_budget/index.html",
    "/zwgk/zdly/czzj/bmyjshsgjf/ys/2026n/index.html",
    "/zwgk/zdly/czzj/bmyjshsgjf/ys/2026n/",
    "/zwgk/fdzdgknr/czxx/czyjs/bmczys/",
    "/zdxxgk/czzjxx/szfbmczyjs/2026/",
    "/zdxxgk/czzjxx/szfbmczyjs/",
    "/zfxxgk/fdzdgknr/czxx/czyjs/",
    "/zwgk/zdlyxxgk/czyjshsgjf/czyjs/",
    "/xw/ztjj/zhszdlyxxgkzl/czysjshsgjfgk/czyjs/",
    "/ztzl/yjsxx/", "/ztzl/czyjs/",
    "/zwgk/zfxxgkzl/fdzdgknr/ysjs/bmczyjsbgjsgjf/bmysnew/2026nbmys/",
    "/zwgk/zfxxgkzl/fdzdgknr/czxx/bmczyjs/",
    "/szf/ztzl/ysgk/2026/",
    "/zwgk/zdlygk/czzj_5858315/sjczyjsjsgjf/",
    "/xxgk/xzwgk/glgk/czxx/szbm/",
]

def probe_budget_page(session, website, city):
    probed = 0
    for path in COMMON_BUDGET_PATHS:
        if probed >= MAX_PROBE_PATHS:
            logger.info(f"  [{city}] 已探测{probed}个路径,达到上限,停止探测")
            break
        url = urljoin(website, path)
        try:
            r = fetch(session, url, timeout=5)
            if r and len(r.text) > 500 and '预算' in r.text:
                logger.info(f"  [{city}] 探测到预算页: {url}")
                return url
        except:
            pass
        probed += 1
        time.sleep(0.2)
    return None

# ========== 处理单个城市 ==========

def process_city(city_info, progress):
    rank = city_info['rank']
    city = city_info['city']
    website = city_info['website']
    budget_url = city_info.get('budget_url') or ''
    city_key = f"{rank:03d}_{city}"
    city_folder = str(BASE_DIR / city_key)

    result = {"found": 0, "downloaded": 0, "depts": [], "missing": []}
    skip_threshold = city_info.get('_skip_threshold', 15)

    try:
        os.makedirs(city_folder, exist_ok=True)

        # ===== 磁盘检测: 已达标就跳过 =====
        existing_count = count_city_files(city_key)
        if existing_count >= skip_threshold:
            logger.info(f"[{city}] 磁盘已有{existing_count}个文件(>={skip_threshold})，跳过")
            update_city_progress(progress, city_key, existing_count, existing_count)
            return {"found": existing_count, "downloaded": existing_count}

        logger.info(f"{'='*50}")
        logger.info(f"[{rank}] {city} (已有{existing_count}个文件)")
        logger.info(f"{'='*50}")

        session = requests.Session()
        session.headers.update(HEADERS)
        session.verify = False

        if not budget_url:
            budget_url = probe_budget_page(session, website, city)
            if not budget_url:
                budget_url = website
                logger.warning(f"  [{city}] 未找到预算专页，用官网首页")

        logger.info(f"  [{city}] 扫描: {budget_url}")
        dept_links = find_dept_budgets(session, budget_url, city)

        if len(dept_links) < 5 and budget_url != website:
            logger.info(f"  [{city}] 找到太少({len(dept_links)})，再扫描官网首页")
            more = find_dept_budgets(session, website, city)
            for dept, links in more.items():
                if dept not in dept_links:
                    dept_links[dept] = links
                else:
                    existing_urls = {u for _, u, _, _ in dept_links[dept]}
                    for item in links:
                        if item[1] not in existing_urls:
                            dept_links[dept].append(item)

        for dept_name, links in dept_links.items():
            try:
                links_sorted = sorted(links, key=lambda x: x[3], reverse=True)
                best = links_sorted[0]
                text, url, is_pdf, score = best

                logger.info(f"  [{city}] ✓ {dept_name} ({len(links)}个链接)")
                result['depts'].append(dept_name)

                clean_dept = safe_filename(dept_name)

                if is_pdf:
                    save_path = os.path.join(city_folder, f"{clean_dept}_2026年部门预算.pdf")
                    if not os.path.exists(save_path):
                        if download_pdf(session, url, save_path):
                            result['downloaded'] += 1
                            logger.info(f"    下载: {os.path.basename(save_path)}")
                        else:
                            logger.warning(f"    下载失败: {url}")
                    else:
                        result['downloaded'] += 1
                else:
                    time.sleep(DELAY_PAGE)
                    detail_r = fetch(session, url)
                    if detail_r:
                        sub_links, _ = extract_all_links(session, url, city)
                        pdf_candidates = []

                        # 方式1: 从链接中查找PDF
                        for st, su, _ in sub_links:
                            if su.lower().endswith('.pdf'):
                                is_main, s = is_main_dept_budget(st, dept_name, city)
                                pdf_candidates.append((st, su, s))

                        # 方式2: 从页面HTML中直接搜索PDF链接
                        if not pdf_candidates:
                            pdf_urls = re.findall(r'(?:href|src)=["\']([^"\']*\.pdf)["\']', detail_r.text, re.I)
                            for pu in pdf_urls:
                                full_pu = urljoin(url, pu)
                                pdf_candidates.append(("", full_pu, 10))

                        # 方式3: 搜索iframe/embed中的PDF
                        if not pdf_candidates:
                            iframe_srcs = re.findall(r'(?:iframe|embed|object)[^>]*src=["\']([^"\']*\.pdf[^"\']*)["\']', detail_r.text, re.I)
                            for isrc in iframe_srcs:
                                full_pu = urljoin(url, isrc)
                                pdf_candidates.append(("", full_pu, 10))

                        if pdf_candidates:
                            pdf_candidates.sort(key=lambda x: x[2], reverse=True)
                            best_pdf = pdf_candidates[0]
                            save_path = os.path.join(city_folder, f"{clean_dept}_2026年部门预算.pdf")
                            if not os.path.exists(save_path):
                                if download_pdf(session, best_pdf[1], save_path):
                                    result['downloaded'] += 1
                                    logger.info(f"    下载: {os.path.basename(save_path)}")
                            else:
                                result['downloaded'] += 1
                        else:
                            hpath = os.path.join(city_folder, f"{clean_dept}_2026年部门预算.html")
                            with open(hpath, 'w', encoding='utf-8') as f:
                                f.write(detail_r.text)
                            result['downloaded'] += 1

                time.sleep(DELAY_PAGE)
            except Exception as e:
                logger.warning(f"  [{city}] 处理{dept_name}异常: {e}")
                continue

        for dept in TARGET_DEPTS:
            if dept not in result['depts']:
                result['missing'].append(dept)

        result['found'] = len(result['depts'])

        try:
            with open(os.path.join(city_folder, "爬取汇总.txt"), 'w', encoding='utf-8') as f:
                f.write(f"城市: {city} (排名{rank})\n")
                f.write(f"官网: {website}\n预算页: {budget_url}\n")
                f.write(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"找到 {result['found']}/32 个部门, 下载 {result['downloaded']} 个文件\n\n")
                f.write("已找到:\n")
                for d in result['depts']:
                    f.write(f"  ✓ {d}\n")
                f.write(f"\n未找到 ({len(result['missing'])}):\n")
                for d in result['missing']:
                    f.write(f"  ✗ {d}\n")
        except Exception as e:
            logger.warning(f"[{city}] 写汇总文件失败: {e}")

    except Exception as e:
        logger.error(f"[{city}] 爬取过程异常: {e}")
        logger.error(traceback.format_exc())

    try:
        if 'completed' not in progress:
            progress['completed'] = {}
        update_city_progress(progress, city_key, result['found'], result['downloaded'])
    except Exception as e:
        logger.error(f"[{city}] 保存进度异常: {e}")

    logger.info(f"  [{city}] 完成: {result['found']}/32部门, {result['downloaded']}文件")
    return result

# ========== 主程序 ==========

WEAK_THRESHOLD = 15  # 达标线


def run(start=1, end=100, retry_weak=False, force=False):
    with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
        cities = json.load(f)
    cities = [c for c in cities if start <= c['rank'] <= end]

    progress = load_progress()
    if 'completed' not in progress:
        progress['completed'] = {}

    # 启动时扫描磁盘，同步进度
    logger.info("扫描磁盘已有文件...")
    disk_done = 0
    for c in cities:
        ck = f"{c['rank']:03d}_{c['city']}"
        n = count_city_files(ck)
        if n >= WEAK_THRESHOLD:
            disk_done += 1
            progress['completed'][ck] = {
                "found": n, "downloaded": n,
                "time": time.strftime('%Y-%m-%d %H:%M:%S')
            }
    save_progress(progress)
    logger.info(f"磁盘已达标: {disk_done}个城市(>={WEAK_THRESHOLD}文件)")

    # 过滤: --retry-weak 只处理弱城市
    if retry_weak:
        weak = []
        for c in cities:
            ck = f"{c['rank']:03d}_{c['city']}"
            n = count_city_files(ck)
            if n < WEAK_THRESHOLD:
                c['_skip_threshold'] = WEAK_THRESHOLD
                weak.append(c)
        ordered = weak
        logger.info(f"--retry-weak: 仅处理{len(weak)}个弱城市(<{WEAK_THRESHOLD}文件)")
    elif force:
        ordered = cities
        for c in ordered:
            c['_skip_threshold'] = 9999  # 强制不跳过
        logger.info(f"--force: 强制处理全部{len(ordered)}个城市")
    else:
        # 有budget_url的优先处理
        with_url = [c for c in cities if c.get('budget_url')]
        without_url = [c for c in cities if not c.get('budget_url')]
        ordered = with_url + without_url

    logger.info(f"开始爬取 {len(ordered)} 个城市, 32个目标部门")
    logger.info(f"已达标: {disk_done}个城市")
    logger.info(f"v12: SSL回退+分页检测+部门匹配增强")

    total_found = 0
    total_downloaded = 0

    for i, city_info in enumerate(ordered):
        city_name = city_info['city']
        try:
            logger.info(f"--- 进度: {i+1}/{len(ordered)} ---")
            result = process_city(city_info, progress)
            if result and isinstance(result, dict):
                total_found += result.get('found', 0)
                total_downloaded += result.get('downloaded', 0)
        except Exception as e:
            logger.error(f"[{city_name}] 处理异常: {e}")
            logger.error(traceback.format_exc())
            try:
                city_key = f"{city_info['rank']:03d}_{city_name}"
                update_city_progress(progress, city_key, 0, 0)
            except:
                pass

        # 每个城市之间休息
        time.sleep(DELAY_CITY)

    save_progress(progress)

    completed_count = len([k for k, v in progress.get('completed', {}).items()
                          if isinstance(v, dict) and v.get('found', 0) > 0])
    logger.info("=" * 60)
    logger.info(f"全部完成! 总计找到 {total_found} 个部门匹配, 下载 {total_downloaded} 个文件")
    logger.info(f"有效完成城市: {completed_count}/100")
    logger.info("=" * 60)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--start', type=int, default=1)
    p.add_argument('--end', type=int, default=100)
    p.add_argument('--city', type=str, default=None)
    p.add_argument('--workers', type=int, default=MAX_WORKERS)
    p.add_argument('--retry-weak', action='store_true', help='仅重爬<15文件的弱城市')
    p.add_argument('--force', action='store_true', help='强制重爬所有城市')
    args = p.parse_args()

    MAX_WORKERS = args.workers

    if args.city:
        with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
            cities = json.load(f)
        ci = next((c for c in cities if c['city'] == args.city), None)
        if ci:
            progress = load_progress()
            process_city(ci, progress)
        else:
            print(f"未找到: {args.city}")
    else:
        run(args.start, args.end, retry_weak=args.retry_weak, force=args.force)
