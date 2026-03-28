#!/usr/bin/env python3
"""
中国百强城市2026年部门预算爬取脚本 v12
核心改进:
  1. 支持分页列表页 (index.html, index_2.html ... index_N.html)
  2. 两跳爬取: 列表页 → 详情页 → PDF附件
  3. 增强部门匹配 (增加口岸办等)
  4. 搜索引擎辅助兜底
  5. 降低跳过阈值, 基于部门覆盖率而非文件数
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
LOG_DIR = Path(__file__).parent / "logs"
LOG_FILE = LOG_DIR / "scrape_v12.log"
PROGRESS_FILE = Path(__file__).parent / "scrape_progress_v12.json"
CITY_DATA_FILE = Path(__file__).parent / "city_data.json"

LOG_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
}
# Rotate User-Agents to reduce blocking
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
]
TIMEOUT = 20
RETRY = 3
MAX_LIST_PAGES = 50       # 最多翻50页列表页
MAX_DETAIL_PER_DEPT = 3   # 每个部门最多进3个详情页找PDF
DELAY_PAGE = 0.3
DELAY_CITY = 2
MIN_DEPT_COVERAGE = 15    # 至少找到15个部门才算合格

# ========== 32+目标委办局 ==========
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

MATCH_RULES = [
    (["公安局交通警察", "公安交通管理", "交警局", "交通警察局", "交警支队", "公安局交通管理", "公安交警", "交警大队", "交通警察支队", "公安局交警", "交通警察总队", "交管局", "公安局交管", "公安交通警察", "交通管理局"], "公安局交通警察局"),
    (["卫生健康委", "卫健委", "卫生健康局", "卫生局"], "卫生健康委员会"),
    (["教育局", "教育委员会", "教育委", "教委", "教育体育局", "教体局"], "教育局"),
    (["发展和改革", "发展改革", "发改委", "发改局"], "发展和改革局"),
    (["规划和自然资源", "自然资源和规划", "自然资源局", "规划局", "国土资源"], "规划和自然资源局"),
    (["交通运输局", "交通运输委", "交通局", "交通委"], "交通运输局"),
    (["科技创新局", "科学技术局", "科技局", "科技创新委"], "科技创新局"),
    (["水务局", "水利局", "水利水务"], "水务局"),
    (["人力资源和社会保障", "人力资源社会保障", "人社局"], "人力资源和社会保障局"),
    (["工业和信息化", "工信局", "经济和信息化", "经信局", "经信委"], "工业和信息化局"),
    (["市场监督管理", "市场监管"], "市场监督管理局"),
    (["国有资产监督管理", "国资委"], "国有资产监督管理委员会"),
    (["口岸办公室", "口岸办", "口岸事务", "口岸管理", "口岸局", "口岸管理局", "口岸和物流", "口岸服务", "口岸工作"], "口岸办公室"),
    (["公安局", "市公安局"], "公安局"),
    (["医疗保障局", "医保局"], "医疗保障局"),
    (["商务局", "商务委"], "商务局"),
    (["文化广电旅游体育", "文化广电旅游", "文化和旅游", "文旅局", "文广旅体", "文化体育旅游", "文广旅局", "文体旅游", "文广新旅", "文化旅游", "文体广旅", "文体广电旅游"], "文化广电旅游体育局"),
    (["生态环境局", "环境保护局", "环保局"], "生态环境局"),
    (["政务服务和数据管理", "政务服务数据管理", "政务服务局", "大数据管理局", "数据局", "行政审批局", "政数局", "大数据发展局", "政务和大数据", "行政审批服务", "数据管理局", "政务服务管理"], "政务服务和数据管理局"),
    (["城市管理和综合执法", "城市管理综合执法", "城市管理局", "城管局", "城管执法", "综合行政执法", "城管和综合执法", "城市管理行政执法", "城市管理委员会", "综合执法局"], "城市管理和综合执法局"),
    (["退役军人事务", "退役军人局"], "退役军人事务局"),
    (["宣传部", "市委宣传", "宣传部门", "中共.*宣传", "党委宣传", "委宣传部", "宣传部（", "宣传部门预算", "宣传部2026", "中共.*市委宣传", "市委.*宣传部", "宣传事务", "党委宣传部", "市委宣传部门预算", "精神文明.*办"], "宣传部"),
    (["司法局"], "司法局"),
    (["住房和建设", "住房和城乡建设", "住建局", "住房建设", "住房城乡建设", "住建委", "住房保障和房屋管理", "住房保障和房管"], "住房和建设局"),
    (["建筑工务署", "建筑工务中心", "建设工程事务", "工务署", "建筑工务", "建设工务署", "建设工务局", "建设工务中心"], "建筑工务署"),
    (["民政局"], "民政局"),
    (["财政局"], "财政局"),
    (["气象局", "气象台", "气象部门", "气象服务", "气象中心", "气象事业", "市气象局", "气象灾害防御", "气象预警"], "气象局"),
    (["应急管理局", "应急局", "安全生产监督"], "应急管理局"),
    (["审计局"], "审计局"),
    (["政府办公厅", "政府办公室", "人民政府办公"], "政府办公厅"),
    (["统计局"], "统计局"),
    (["信访局", "信访办", "信访办公室", "信访事务中心", "群众来访接待"], "信访局"),
]

# 子单位关键词 - 用于过滤掉下属单位
SUB_UNIT_KEYWORDS = [
    "中心", "研究院", "研究所", "学校", "学院", "医院", "站",
    "大队", "支队", "总队", "干部", "老干", "活动室", "活动中心",
    "服务中心", "事务中心", "管理所", "监测", "培训", "幼儿园",
    "纪念馆", "博物馆", "图书馆", "文化馆", "美术馆", "展示馆",
    "福利院", "救助", "戒毒", "疗养", "供应站", "试验场",
    "分局", "监管局",
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

# ========== 进度管理 ==========

def load_progress():
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"completed": {}, "failed": [], "strategies": {}}
        if not isinstance(data, dict):
            return {"completed": {}, "failed": [], "strategies": {}}
        data.setdefault('completed', {})
        data.setdefault('failed', [])
        data.setdefault('strategies', {})
        return data
    return {"completed": {}, "failed": [], "strategies": {}}

def save_progress(progress_data):
    try:
        tmp = str(PROGRESS_FILE) + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, str(PROGRESS_FILE))
    except Exception as e:
        logger.error(f"保存进度失败: {e}")

def update_city_progress(progress_data, city_key, found_depts, downloaded, strategy_used="default"):
    progress_data['completed'][city_key] = {
        "found": len(found_depts) if isinstance(found_depts, list) else found_depts,
        "depts": found_depts if isinstance(found_depts, list) else [],
        "downloaded": downloaded,
        "strategy": strategy_used,
        "time": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    save_progress(progress_data)

# ========== 核心工具 ==========

def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    return session

import random

def fetch(session, url, timeout=TIMEOUT):
    # Rotate UA per request
    session.headers['User-Agent'] = random.choice(USER_AGENTS)
    urls_to_try = [url]
    if url.startswith('https://'):
        urls_to_try.append(url.replace('https://', 'http://', 1))
    elif url.startswith('http://'):
        urls_to_try.append(url.replace('http://', 'https://', 1))

    for try_url in urls_to_try:
        for i in range(RETRY):
            try:
                r = session.get(try_url, timeout=timeout, allow_redirects=True, verify=False)
                r.encoding = r.apparent_encoding or 'utf-8'
                if r.status_code == 200:
                    return r
                if r.status_code in (403, 503):
                    # May be rate-limited, wait and retry
                    time.sleep(1 + i)
                    continue
                if r.status_code >= 400:
                    break  # Don't retry client errors other than 403
            except requests.exceptions.SSLError:
                break  # Try next URL (http/https switch)
            except requests.exceptions.ConnectionError:
                if i < RETRY - 1:
                    time.sleep(1 + i)
            except Exception:
                if i < RETRY - 1:
                    time.sleep(1)
    return None

def match_dept(text, city=""):
    if not text or len(text.strip()) < 2:
        return None
    t = text.strip()
    # 去掉城市前缀
    for p in [city + "市", city, "市"]:
        if p and t.startswith(p):
            t = t[len(p):]
    for keywords, dept in MATCH_RULES:
        for kw in keywords:
            if '.*' in kw:
                if re.search(kw, t):
                    return dept
            elif kw in t:
                if dept == "公安局" and ("交通" in t or "交警" in t):
                    return "公安局交通警察局"
                return dept
    return None

def is_main_dept_budget(text, dept_name, city=""):
    """判断是否为部门本级预算(非下属单位)"""
    t = text.strip()
    score = 0

    # 必须含预算
    if "预算" not in t and "budget" not in t.lower():
        return False, 0

    if "部门预算" in t:
        score += 100
    elif "预算" in t:
        score += 30

    # 匹配部门名
    for keywords, dept in MATCH_RULES:
        if dept == dept_name:
            for kw in keywords:
                if '.*' in kw:
                    if re.search(kw, t):
                        score += 50
                        break
                elif kw in t:
                    score += 50
                    break
            break

    if "2026" in t:
        score += 20

    # 含"本级"加分(说明是主体)
    if "本级" in t:
        score += 30

    # 子单位扣分
    if "部门预算" not in t:
        for kw in SUB_UNIT_KEYWORDS:
            if kw in t:
                score -= 80
                break

    return score > 0, score

def download_pdf(session, url, path):
    urls_to_try = [url]
    if url.startswith('https://'):
        urls_to_try.append(url.replace('https://', 'http://', 1))
    for try_url in urls_to_try:
        try:
            r = session.get(try_url, timeout=60, stream=True, verify=False)
            if r.status_code == 200:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                if os.path.getsize(path) < 1000:
                    os.remove(path)
                    return False
                return True
        except:
            continue
    return False

def safe_filename(s, maxlen=80):
    return re.sub(r'[\\/:*?"<>|\s]', '_', s)[:maxlen]

# ========== 分页列表爬取 (v12核心改进) ==========

def detect_pagination(soup, base_url):
    """检测分页模式, 返回总页数和URL模板"""
    # 模式1: index_N.html / index_N.shtml (深圳等)
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text in ['尾页', '末页', '最后一页']:
            m = re.search(r'index_(\d+)', href)
            if m:
                total = int(m.group(1))
                return total, 'index_N'

    # 模式1b: column-index-N.shtml (成都等)
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text in ['尾页', '末页', '最后一页', '下一页', '>>']:
            m = re.search(r'column-index-(\d+)', href)
            if m:
                total = int(m.group(1))
                return total, 'column_index_N'
    # 也从所有链接中找column-index模式
    col_idx_nums = []
    for a in soup.find_all('a', href=True):
        m = re.search(r'column-index-(\d+)', a['href'])
        if m:
            col_idx_nums.append(int(m.group(1)))
    if col_idx_nums:
        return max(col_idx_nums), 'column_index_N'

    # 模式1c: t_N.html / t/N.html (一些政府网站)
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text in ['尾页', '末页', '最后一页', '下一页', '>>']:
            m = re.search(r't[/_](\d+)\.html', href)
            if m:
                total = int(m.group(1))
                return total, 'page_param'

    # 模式2: ?page=N 或 &page=N 或 ?PageIndex=N 或 ?pageNo=N
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text in ['尾页', '末页', '最后一页']:
            m = re.search(r'[?&](?:page|PageIndex|pageNo|pageNum|currPage|pn|p)=(\d+)', href, re.I)
            if m:
                return int(m.group(1)), 'page_param'
    # 模式2 fallback: 从"下一页"/">"链接推断, 至少有2页
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text in ['下一页', '>', '>>', '›', '»']:
            m = re.search(r'[?&](?:page|PageIndex|pageNo|pageNum|currPage|pn|p)=(\d+)', href, re.I)
            if m:
                # 下一页链接指向的页码作为下限, 估算至少有10页
                next_page = int(m.group(1))
                return max(next_page * 5, 10), 'page_param'

    # 模式2b: /N.html 数字路径分页 (如 /1.html, /2.html)
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text in ['尾页', '末页', '最后一页']:
            m = re.search(r'/(\d+)\.html', href)
            if m:
                total = int(m.group(1))
                return total, 'num_html'

    # 模式3: 从JS中提取总页数
    for script in soup.find_all('script'):
        if script.string:
            # contenpage/totalPage/pageCount/countPage 等变量
            m = re.search(r'(?:contenpage|totalPage|pageCount|countPage|pages)\s*[=:]\s*(\d+)', script.string)
            if m:
                total = int(m.group(1))
                # 判断URL模式
                if 'column-index' in str(soup):
                    return min(total, 200), 'column_index_N'
                if 'index_' in str(soup):
                    return min(total, 200), 'index_N'
                return min(total, 200), 'page_param'

    # 模式3b: 从JS中查找 var countPage 或 createPageHTML(N) 模式
    for script in soup.find_all('script'):
        if script.string:
            m = re.search(r'createPageHTML\s*\(\s*(\d+)', script.string)
            if m:
                total = int(m.group(1))
                if 'index_' in str(soup):
                    return min(total, 200), 'index_N'
                return min(total, 200), 'page_param'

    # 模式4: 数字页码链接
    page_nums = []
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        if text.isdigit() and int(text) > 1:
            page_nums.append(int(text))
    if page_nums:
        max_visible = max(page_nums)
        # 尝试判断URL模式
        for a in soup.find_all('a', href=True):
            if a.get_text(strip=True) == str(max_visible):
                href = a['href']
                if 'column-index' in href:
                    return max_visible, 'column_index_N'
                if 'index_' in href:
                    return max_visible, 'index_N'
                if 'page=' in href:
                    return max_visible, 'page_param'
                m = re.search(r'/(\d+)\.html', href)
                if m:
                    return max_visible, 'num_html'

    return 1, 'single'

def build_page_url(base_url, page_num, pattern):
    """根据分页模式构建第N页URL"""
    if pattern == 'index_N':
        if page_num == 1:
            return re.sub(r'index(_\d+)?\.(html|shtml)', r'index.\2', base_url)
        base = re.sub(r'index(_\d+)?\.(html|shtml)', f'index_{page_num}.\\2', base_url)
        if base == base_url and not re.search(r'\.(html|shtml)$', base_url):
            base = base_url.rstrip('/') + f'/index_{page_num}.html'
        return base
    elif pattern == 'column_index_N':
        if page_num == 1:
            return re.sub(r'column-index-\d+', 'column-index-1', base_url)
        return re.sub(r'column-index-\d+', f'column-index-{page_num}', base_url)
    elif pattern == 'page_param':
        parsed = urlparse(base_url)
        if 'page=' in parsed.query:
            return re.sub(r'page=\d+', f'page={page_num}', base_url)
        sep = '&' if parsed.query else '?'
        return f"{base_url}{sep}page={page_num}"
    elif pattern == 'num_html':
        # /1.html -> /2.html
        return re.sub(r'/\d+\.html$', f'/{page_num}.html', base_url)
    return base_url

def extract_links_from_page(soup, base_url, city):
    """从一个页面提取所有链接及其部门匹配"""
    results = []
    seen = set()
    for a in soup.find_all('a', href=True):
        text = a.get_text(strip=True)
        href = a['href'].strip()
        if not text or not href or href.startswith('#') or href.startswith('javascript'):
            continue
        full = urljoin(base_url, href)
        if full in seen:
            continue
        seen.add(full)
        dept = match_dept(text, city)
        # Also check title attribute if text didn't match
        if not dept:
            title_attr = a.get('title', '')
            if title_attr:
                dept = match_dept(title_attr, city)
                if dept:
                    text = title_attr  # use title for scoring
        is_pdf = full.lower().endswith('.pdf')
        results.append({
            'text': text,
            'url': full,
            'dept': dept,
            'is_pdf': is_pdf,
        })
    return results

# ========== 策略1: 分页列表爬取 ==========

def strategy_paginated_list(session, budget_url, city, needed_depts):
    """遍历分页列表, 进详情页找PDF"""
    logger.info(f"  [{city}] 策略1: 分页列表爬取 {budget_url}")
    found = {}  # dept -> [(text, url, is_pdf, score)]

    # 第一页
    r = fetch(session, budget_url)
    if not r:
        # 尝试加index.html
        alt = budget_url.rstrip('/') + '/index.html'
        r = fetch(session, alt)
        if r:
            budget_url = alt
    if not r and budget_url.startswith('https://'):
        # 尝试http回退
        http_url = budget_url.replace('https://', 'http://', 1)
        r = fetch(session, http_url)
        if r:
            budget_url = http_url
    if not r:
        # 尝试index.shtml
        alt = budget_url.rstrip('/') + '/index.shtml'
        r = fetch(session, alt)
        if r:
            budget_url = alt
    if not r:
        # 尝试去掉末尾路径层级
        parsed = urlparse(budget_url)
        parent = parsed.path.rstrip('/').rsplit('/', 1)[0] + '/'
        if parent != parsed.path:
            parent_url = f"{parsed.scheme}://{parsed.netloc}{parent}"
            r = fetch(session, parent_url)
            if r and '预算' in (r.text or ''):
                budget_url = parent_url
            else:
                r = None
    if not r:
        logger.warning(f"  [{city}] 无法访问预算页")
        return found

    try:
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return found

    total_pages, pattern = detect_pagination(soup, budget_url)
    logger.info(f"  [{city}] 检测到 {total_pages} 页, 模式: {pattern}")

    # 确保base_url有index.html/shtml用于替换
    if pattern == 'index_N' and not re.search(r'\.(html|shtml)$', budget_url):
        budget_url = budget_url.rstrip('/') + '/index.html'
    if pattern == 'column_index_N' and 'column-index' not in budget_url:
        budget_url = budget_url.rstrip('/') + '/column-index-1.shtml'

    pages_to_scan = min(total_pages, MAX_LIST_PAGES)

    for page in range(1, pages_to_scan + 1):
        # 检查是否已找到所有需要的部门
        if needed_depts and all(d in found for d in needed_depts):
            logger.info(f"  [{city}] 所有目标部门已找到, 停止翻页")
            break

        if page == 1:
            page_soup = soup  # 已经有了
        else:
            page_url = build_page_url(budget_url, page, pattern)
            time.sleep(DELAY_PAGE)
            pr = fetch(session, page_url)
            if not pr:
                continue
            try:
                page_soup = BeautifulSoup(pr.text, 'html.parser')
            except:
                continue

        links = extract_links_from_page(page_soup, budget_url, city)

        for link in links:
            dept = link['dept']
            if not dept:
                continue
            if needed_depts and dept not in needed_depts:
                continue

            is_main, score = is_main_dept_budget(link['text'], dept, city)
            if dept not in found:
                found[dept] = []
            found[dept].append((link['text'], link['url'], link['is_pdf'], score))

        if page % 10 == 0:
            logger.info(f"  [{city}] 已扫描 {page}/{pages_to_scan} 页, 找到 {len(found)} 个部门")

    logger.info(f"  [{city}] 列表扫描完成: {len(found)} 个部门")
    return found

# ========== 策略2: 详情页提取PDF ==========

def extract_pdf_from_detail(session, detail_url, dept_name, city):
    """进入详情页, 提取PDF附件链接"""
    r = fetch(session, detail_url)
    if not r:
        return None

    try:
        soup = BeautifulSoup(r.text, 'html.parser')
    except:
        return None

    pdf_candidates = []

    # 方法1: 直接找PDF链接
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        text = a.get_text(strip=True)
        full = urljoin(detail_url, href)
        if full.lower().endswith('.pdf'):
            is_main, score = is_main_dept_budget(text, dept_name, city)
            # 如果文件名含部门关键词加分
            fname = unquote(full.split('/')[-1])
            for keywords, dept in MATCH_RULES:
                if dept == dept_name:
                    for kw in keywords:
                        if kw in fname:
                            score += 30
                            break
                    break
            if "部门预算" in fname or "部门预算" in text:
                score += 50
            pdf_candidates.append((text or fname, full, score))

    # 方法2: iframe中嵌入的PDF
    for iframe in soup.find_all('iframe', src=True):
        src = iframe['src']
        if src.lower().endswith('.pdf'):
            full = urljoin(detail_url, src)
            pdf_candidates.append(("iframe_pdf", full, 10))

    # 方法3: embed/object标签中的PDF
    for tag in soup.find_all(['embed', 'object']):
        src = tag.get('src') or tag.get('data') or ''
        if src.lower().endswith('.pdf'):
            full = urljoin(detail_url, src)
            pdf_candidates.append(("embed_pdf", full, 10))

    # 方法4: 从onclick/href中提取PDF URL
    for a in soup.find_all(['a', 'span', 'div'], onclick=True):
        onclick = a['onclick']
        m = re.search(r"['\"]([^'\"]*\.pdf)['\"]", onclick, re.I)
        if m:
            full = urljoin(detail_url, m.group(1))
            pdf_candidates.append(("onclick_pdf", full, 5))

    # 方法5: 从页面文本中正则提取PDF链接
    if not pdf_candidates:
        pdf_urls_in_text = re.findall(r'(?:href|src|url)\s*[=:]\s*["\']?([^"\'>\s]+\.pdf)', r.text, re.I)
        for purl in pdf_urls_in_text[:5]:
            full = urljoin(detail_url, purl)
            pdf_candidates.append(("regex_pdf", full, 3))

    # 方法6: 查找PDF预览器URL (如 /pdfjs/viewer?file=xxx.pdf)
    if not pdf_candidates:
        viewer_matches = re.findall(r'[?&]file=([^&"\'>\s]+\.pdf)', r.text, re.I)
        for purl in viewer_matches[:3]:
            full = urljoin(detail_url, unquote(purl))
            pdf_candidates.append(("viewer_pdf", full, 5))

    # 方法7: window.open 或 location.href 中的PDF
    if not pdf_candidates:
        js_matches = re.findall(r'(?:window\.open|location\.href)\s*[=(]\s*["\']([^"\']+\.pdf)', r.text, re.I)
        for purl in js_matches[:3]:
            full = urljoin(detail_url, purl)
            pdf_candidates.append(("js_pdf", full, 4))

    if not pdf_candidates:
        return None

    # 选得分最高的
    pdf_candidates.sort(key=lambda x: x[2], reverse=True)
    return pdf_candidates[0][1]  # 返回URL

# ========== 策略2: 搜索接口 (逐个委办局搜索) ==========

SEARCH_URL_PATTERNS = [
    "/search?searchWord={query}",
    "/search?q={query}",
    "/search.html?searchWord={query}",
    "/was5/web/search?searchword={query}",
    "/was5/web/search?channelid=&searchword={query}",
    "/jrobotfront/search.action?webid=1&searchWord={query}",
    "/site/tpl/szfbmczyjs/?searchWord={query}",
    "/search.do?searchWord={query}",
    "/search?keyword={query}",
    "/search?wd={query}",
    "/search.jsp?keyword={query}",
    "/search.aspx?q={query}",
    "/opensearch?searchWord={query}",
    "/fullsearch?q={query}",
    "/site/zgnj/search.html?searchWord={query}&siteId=10",
    "/igs/front/search.html?searchWord={query}",
    "/search/index.html?searchWord={query}",
    "/search/index.do?searchWord={query}",
    "/search_result.jsp?q={query}",
    "/search?key={query}",
    "/search?words={query}",
    "/search?content={query}",
    "/search/index.jsp?searchWord={query}",
    "/was5/web/search?searchWord={query}",
    "/search?type=0&searchWord={query}",
    "/search.html?keyword={query}",
    "/search.html?q={query}",
    "/search/index?searchWord={query}",
    "/fullTextSearch?key={query}",
    "/search?text={query}",
    "/search.shtml?searchWord={query}",
    "/search/data?searchWord={query}",
    "/search.html?keyword={query}",
    "/search?channelId=0&searchWord={query}",
    "/jrobotfront/search.action?searchWord={query}",
    "/search?appid=1&searchWord={query}",
    "/search/s?searchWord={query}",
    "/search/index.html?keyword={query}",
    "/search/index.html?q={query}",
    "/search?tab=all&searchWord={query}",
    "/search.html?word={query}",
    "/search?kw={query}",
    "/search/list?keyword={query}",
    "/was5/web/search?searchword={query}&channelid=0",
    "/jrobotfront/search.action?webid=1&pg=1&searchWord={query}",
]

def detect_search_endpoint(session, website, city):
    """自动探测城市官网的搜索接口"""
    # 先检查首页有没有搜索表单
    r = fetch(session, website, timeout=10)
    if r:
        try:
            soup = BeautifulSoup(r.text, 'html.parser')
            for form in soup.find_all('form'):
                action = form.get('action', '')
                if 'search' in action.lower():
                    # 找搜索参数名
                    for inp in form.find_all('input'):
                        name = inp.get('name', '')
                        if name and name.lower() in ('searchword', 'q', 'keyword', 'wd', 'searchcontent', 'key', 'kw', 'words', 'search', 'query'):
                            search_base = urljoin(website, action)
                            return f"{search_base}?{name}={{query}}"
        except:
            pass

    # 也检查页面中的搜索链接 (如 /search 开头的链接)
    if r:
        try:
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                if '/search' in href.lower() and 'searchword' not in href.lower():
                    search_base = urljoin(website, href.split('?')[0])
                    test_url = f"{search_base}?searchWord={quote('2026年预算')}"
                    tr = fetch(session, test_url, timeout=8)
                    if tr and len(tr.text) > 1000 and ('搜索' in tr.text or '结果' in tr.text or '预算' in tr.text):
                        logger.info(f"  [{city}] 探测到搜索接口(链接): {search_base}?searchWord={{query}}")
                        return f"{search_base}?searchWord={{query}}"
        except:
            pass

    # 尝试常见模式 (只试前15个最常见的, 减少探测时间)
    test_query = quote("2026年预算")
    for pattern in SEARCH_URL_PATTERNS[:15]:
        url = urljoin(website, pattern.format(query=test_query))
        try:
            r = fetch(session, url, timeout=5)
            if r and len(r.text) > 1000 and ('搜索' in r.text or '结果' in r.text or '预算' in r.text):
                logger.info(f"  [{city}] 探测到搜索接口: {pattern}")
                return urljoin(website, pattern)
        except:
            pass
        time.sleep(0.1)

    return None

def strategy_search(session, website, city, needed_depts):
    """逐个委办局搜索: 委办局名+2026年预算"""
    found = {}

    search_template = detect_search_endpoint(session, website, city)
    if not search_template:
        logger.info(f"  [{city}] 未找到搜索接口, 跳过搜索策略")
        return found

    logger.info(f"  [{city}] 搜索策略: 逐个搜索 {len(needed_depts)} 个委办局")

    for dept in needed_depts:
        if dept in found:
            continue

        # 获取搜索关键词(用第一个匹配关键词)
        kw = dept
        for keywords, target_dept in MATCH_RULES:
            if target_dept == dept:
                kw = keywords[0]
                break

        query = f"{kw} 2026年预算"
        search_url = search_template.format(query=quote(query))
        if '{query}' not in search_template:
            # 备用: 直接拼接
            search_url = urljoin(website, f"/search?searchWord={quote(query)}")

        time.sleep(DELAY_PAGE)
        r = fetch(session, search_url, timeout=10)
        if not r or len(r.text) < 500:
            continue

        try:
            soup = BeautifulSoup(r.text, 'html.parser')
        except:
            continue

        candidates = []
        for a in soup.find_all('a', href=True):
            text = a.get_text(strip=True)
            if not text or len(text) < 5:
                continue
            href = a['href'].strip()
            if href.startswith('#') or href.startswith('javascript'):
                continue

            full = urljoin(search_url, href)
            matched = match_dept(text, city)

            if matched == dept:
                is_main, score = is_main_dept_budget(text, dept, city)
                is_pdf = full.lower().endswith('.pdf')
                candidates.append((text, full, is_pdf, score))
            elif not matched and dept.replace('局', '') in text and '预算' in text:
                # 宽松匹配
                is_pdf = full.lower().endswith('.pdf')
                candidates.append((text, full, is_pdf, 10))

        if candidates:
            found[dept] = candidates
            logger.info(f"  [{city}] 搜索找到 {dept}: {len(candidates)}个候选")

    logger.info(f"  [{city}] 搜索策略完成: 找到 {len(found)}/{len(needed_depts)} 个部门")
    return found

# ========== 预算路径探测 (同v11) ==========

COMMON_BUDGET_PATHS = [
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
    "/zwgk/zdly/czxx/czyjs/",
    "/zfxxgk/fdzdgknr/bmczyjs/",
    "/zfxxgk/fdzdgknr/bmczyjs/2026/",
    "/zwgk/czxx/bmczyjs/",
    "/zwgk/czxx/bmczyjs/2026/",
    "/czyjs/", "/bmys/", "/czys/",
    "/zfxxgk/zdlyxxgk/czyjshsg/bmyjs/",
    "/zwgk/zfxxgk/czxx/",
    "/ztzl/yjsgk/", "/ztzl/ysgk/",
    "/col/col_budget/index.html",
    "/gkml/czyjs/",
    "/gkml/czyjs/column-index-1.shtml",
    "/zdxxgk/czzjxx/szfbmczyjs/",
    "/zdxxgk/czzjxx/szfbmczyjs/2026/",
    "/zwgk/zdly/czxx/bmczyjs/2026n/",
    "/zwgk/czxx/czyjs/bmczys/",
    "/zwgk/fdzdgknr/czxx/czyjs/",
    "/zwgk/fdzdgknr/czxx/czyjs/bmczys/",
    "/columns/zhuzhan/tsczxx/",
    "/zwgk/fdzdgknr/ysjs/bmczyjsbgjsgjf/bmysnew/2026nbmys/",
    "/zdlyxxgk/czyjshsgjf/czyjs/",
    "/zdlyxxgk/czyjshsg/bmyjs/",
    "/zwgk/zdly/czxx/bmczyjs/index.html",
    "/zwgk/zdly/czxx/bmczyjs/index.shtml",
    "/zfxxgk/fdzdgknr/czxx/bmczyjs/index.html",
    "/zwgk/zfxxgkzl/fdzdgknr/czxx/bmczyjs/",
    "/zwgk/zfxxgkzl/fdzdgknr/ysjs/bmczyjsbgjsgjf/",
    "/xxgk/fdzdgk/czxx/bmczyjs/2026/",
    "/zwgk/zdly/czsj/bmys/",
    "/zwgk/zdly/czsj/czyjs/",
    "/zwgk/zdly/czxx/czyjs/2026/",
    "/zwgk/zdly/czxx/bmysjsgk/",
    "/zwgk/zdzl/czyjs/",
    "/zwgk/zdly/czzj/bmys/",
    "/zfxxgk/zdgk/czxx/bmys/",
    "/zfxxgk/zdgk/czxx/bmczyjs/",
    "/zwgk/zdly/czxx/bmyjs/",
    "/zwgk/zdly/czxx/bmyjs/2026/",
    "/zwgk/czsj/bmczyjs/",
    "/zwgk/czsj/bmczyjs/2026/",
    "/czj/xxgk/bmys/",
    "/czj/xxgk/bmys/2026/",
    "/czj/ysjs/",
    "/czj/ysjs/bmys/",
    "/openness/detail/content/",
    "/col/col_bmys/index.html",
    "/xxgk/bmys/",
    "/xxgk/bmys/2026/",
    "/zfxxgk/fdzdgknr/czxx/czyjs/bmczys/2026/",
    "/zwgk/czxx/yjshsg/bmczys/",
    "/zwgk/czxx/yjshsg/bmczys/2026/",
    "/zwgk/zfxxgk/czxx/bmczyjs/2026/",
    "/zwgk/zdly/czxx/bmczyjs/index_1.shtml",
    "/zwgk/zdly/czgk/bmys/",
    "/zwgk/zdly/czgk/bmys/2026/",
    "/zfxxgk/fdzdgknr/czxx/bmczyjs/index.shtml",
    "/col/col_bmys/",
    "/col/col_czyjs/index.html",
    "/zfxxgk/czxx/czyjs/bmczys/",
    "/zfxxgk/czxx/czyjs/bmczys/2026/",
    "/zwgk/czxx/yjshsg/bmczys/index.html",
    "/zwgk/zdly/czzj/bmczyjs/2026n/",
    "/xxgk/czgk/bmys/",
    "/xxgk/czgk/bmys/2026/",
    "/zfxxgk/fdzdgknr/czxx/czyjs/bmczys/index.html",
    "/zwgk/bmys/2026/",
    "/zwgk/ysjs/bmys/",
    "/zwgk/ysjs/bmys/2026/",
    # 新增路径 - 基于常见政府网站结构
    "/zfxxgk/fdzdgknr/czyjshsg/bmczys/",
    "/zfxxgk/fdzdgknr/czyjshsg/bmczys/2026/",
    "/zwgk/zdly/czxx/bmys/2026/",
    "/zwgk/zdly/czxx/yjsgk/",
    "/zwgk/zdly/czxx/yjsgk/2026/",
    "/zfxxgk/zdlyxxgk/czxx/bmczyjs/",
    "/zfxxgk/zdlyxxgk/czxx/bmczyjs/2026/",
    "/zwgk/zdly/czzj/yjsgk/",
    "/zwgk/zdly/czzj/yjsgk/2026/",
    "/szf/ztzl/ysgk/bmys/",
    "/szf/ztzl/ysgk/bmys/2026/",
    "/zwgk/zfxxgk/fdzdgknr/czxx/bmczyjs/",
    "/zfxxgk/fdzdgknr/bmys/",
    "/zfxxgk/fdzdgknr/bmys/2026/",
    "/xxgk/czxx/bmczyjs/",
    "/xxgk/czxx/bmczyjs/2026/",
    "/xxgk/fdzdgk/czyjs/",
    "/xxgk/fdzdgk/czyjs/2026/",
    "/zwgk/fdzdgknr/ysjs/bmys/",
    "/zwgk/fdzdgknr/ysjs/bmys/2026/",
]

def probe_budget_page(session, website, city):
    # Try standard paths on the main website (limit to first 50 for speed)
    for path in COMMON_BUDGET_PATHS[:50]:
        url = urljoin(website, path)
        try:
            r = fetch(session, url, timeout=6)
            if r and len(r.text) > 500 and '预算' in r.text:
                logger.info(f"  [{city}] 探测到预算页: {url}")
                return url
        except:
            pass
        time.sleep(0.1)

    # Try financial bureau subdomain (czj.xxx.gov.cn or cz.xxx.gov.cn)
    parsed = urlparse(website)
    domain = parsed.netloc.replace('www.', '')
    cz_domains = [f"czj.{domain}", f"cz.{domain}", f"caizj.{domain}"]
    cz_paths = ["/", "/zwgk/", "/xxgk/bmys/", "/ysjs/", "/ysgk/",
                "/zwgk/czyjsgk/", "/xxgk/czsj/list.html",
                "/zwgk_53713/yjsgktypt/ysgk/2026bmys/",
                "/xxgk/bmys/2026/", "/zwgk/bmys/", "/zwgk/bmys/2026/",
                "/zfxxgk/fdzdgknr/czxx/bmczyjs/",
                "/xxgk/gkml/czyjs/", "/ysgkpt/",
                "/gkml/czyjs/", "/bmys/", "/czyjs/",
                "/zwgk/zdly/czxx/bmczyjs/",
                "/zwgk/zdly/czzj/bmczyjs/",
                "/zfxxgk/fdzdgknr/ysjs/bmczyjsbgjsgjf/bmysnew/2026nbmys/",
                "/zwgk/fdzdgknr/czxx/bmczyjs/",
                "/zfxxgk/zdlyxxgk/czxx/bmczyjs/",
                "/zfxxgk/zdlyxxgk/czyjshsg/bmyjs/"]
    for cz_domain in cz_domains:
        for cz_path in cz_paths:
            cz_url = f"http://{cz_domain}{cz_path}"
            try:
                r = fetch(session, cz_url, timeout=8)
                if r and len(r.text) > 500 and '预算' in r.text:
                    logger.info(f"  [{city}] 探测到财政局预算页: {cz_url}")
                    return cz_url
            except:
                pass
            time.sleep(0.2)

    return None

# ========== 处理单个城市 ==========

def process_city(city_info, progress, force=False):
    rank = city_info['rank']
    city = city_info['city']
    website = city_info['website']
    budget_url = city_info.get('budget_url') or ''
    city_key = f"{rank:03d}_{city}"
    city_folder = str(BASE_DIR / city_key)

    os.makedirs(city_folder, exist_ok=True)

    # 检查已有进度
    if not force:
        prev = progress.get('completed', {}).get(city_key, {})
        if isinstance(prev, dict) and prev.get('found', 0) >= MIN_DEPT_COVERAGE:
            logger.info(f"[{city}] 已有 {prev['found']} 个部门, 跳过")
            return prev

    logger.info(f"{'='*50}")
    logger.info(f"[{rank}] {city}")
    logger.info(f"{'='*50}")

    session = create_session()

    # 确定预算页URL
    if not budget_url:
        budget_url = probe_budget_page(session, website, city)
        if not budget_url:
            budget_url = website
            logger.warning(f"  [{city}] 未找到预算专页, 用官网首页")

    # 需要找的部门
    needed_depts = set(TARGET_DEPTS)

    # 检查已下载的文件, 排除已有的部门
    existing_depts = set()
    try:
        for f in Path(city_folder).iterdir():
            if f.suffix in ('.pdf', '.html') and f.name != '.gitkeep' and f.name != '爬取汇总.txt':
                if f.stat().st_size > 1000:
                    for dept in TARGET_DEPTS:
                        clean = safe_filename(dept)
                        if f.name.startswith(clean):
                            existing_depts.add(dept)
    except:
        pass

    if existing_depts:
        logger.info(f"  [{city}] 已有文件覆盖 {len(existing_depts)} 个部门")
        needed_depts -= existing_depts

    # ===== 策略1: 分页列表爬取 =====
    dept_links = strategy_paginated_list(session, budget_url, city, needed_depts)

    # 如果找到太少, 试搜索
    found_new = set(dept_links.keys())
    still_missing = needed_depts - found_new - existing_depts
    strategy_used = "paginated_list"

    if len(still_missing) > 3:
        # ===== 策略2: 搜索接口 =====
        logger.info(f"  [{city}] 还缺 {len(still_missing)} 个部门, 启用搜索策略")
        search_results = strategy_search(session, website, city, still_missing)
        for dept, links in search_results.items():
            if dept not in dept_links:
                dept_links[dept] = links
        strategy_used = "paginated_list+search"

    # ===== 下载PDF =====
    downloaded = 0
    found_depts = list(existing_depts)

    for dept_name, links in dept_links.items():
        try:
            # 按score排序, 优先主体预算
            links_sorted = sorted(links, key=lambda x: x[3], reverse=True)

            clean_dept = safe_filename(dept_name)
            save_path = os.path.join(city_folder, f"{clean_dept}_2026年部门预算.pdf")

            # 已有则跳过
            if os.path.exists(save_path) and os.path.getsize(save_path) > 1000:
                found_depts.append(dept_name)
                downloaded += 1
                continue

            pdf_downloaded = False

            for text, url, is_pdf, score in links_sorted[:MAX_DETAIL_PER_DEPT]:
                if is_pdf:
                    # 直接PDF链接
                    if download_pdf(session, url, save_path):
                        logger.info(f"  [{city}] ✓ {dept_name} (直接PDF)")
                        found_depts.append(dept_name)
                        downloaded += 1
                        pdf_downloaded = True
                        break
                else:
                    # 进详情页找PDF
                    time.sleep(DELAY_PAGE)
                    pdf_url = extract_pdf_from_detail(session, url, dept_name, city)
                    if pdf_url:
                        if download_pdf(session, pdf_url, save_path):
                            logger.info(f"  [{city}] ✓ {dept_name} (详情页PDF)")
                            found_depts.append(dept_name)
                            downloaded += 1
                            pdf_downloaded = True
                            break

            if not pdf_downloaded:
                # 保存为HTML兜底
                if links_sorted:
                    r = fetch(session, links_sorted[0][1])
                    if r:
                        hpath = os.path.join(city_folder, f"{clean_dept}_2026年部门预算.html")
                        with open(hpath, 'w', encoding='utf-8') as f:
                            f.write(r.text)
                        found_depts.append(dept_name)
                        downloaded += 1

            time.sleep(DELAY_PAGE)
        except Exception as e:
            logger.warning(f"  [{city}] 处理{dept_name}异常: {e}")

    # 去重
    found_depts = list(set(found_depts))
    missing = [d for d in TARGET_DEPTS if d not in found_depts]

    # 写汇总
    try:
        with open(os.path.join(city_folder, "爬取汇总.txt"), 'w', encoding='utf-8') as f:
            f.write(f"城市: {city} (排名{rank})\n")
            f.write(f"官网: {website}\n预算页: {budget_url}\n")
            f.write(f"策略: {strategy_used}\n")
            f.write(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"找到 {len(found_depts)}/{len(TARGET_DEPTS)} 个部门, 下载 {downloaded} 个文件\n\n")
            f.write("已找到:\n")
            for d in sorted(found_depts):
                f.write(f"  ✓ {d}\n")
            f.write(f"\n未找到 ({len(missing)}):\n")
            for d in missing:
                f.write(f"  ✗ {d}\n")
    except:
        pass

    update_city_progress(progress, city_key, found_depts, downloaded, strategy_used)
    logger.info(f"  [{city}] 完成: {len(found_depts)}/{len(TARGET_DEPTS)}部门, {downloaded}文件, 策略:{strategy_used}")

    return {"found": len(found_depts), "downloaded": downloaded, "depts": found_depts, "missing": missing}

# ========== 主程序 ==========

def run(start=1, end=100, force=False, retry_weak=False):
    with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
        cities = json.load(f)

    progress = load_progress()

    if retry_weak:
        # 只重试覆盖率不够的城市
        weak_cities = []
        for c in cities:
            if start <= c['rank'] <= end:
                ck = f"{c['rank']:03d}_{c['city']}"
                prev = progress.get('completed', {}).get(ck, {})
                if not isinstance(prev, dict) or prev.get('found', 0) < MIN_DEPT_COVERAGE:
                    weak_cities.append(c)
        cities = weak_cities
        logger.info(f"重试模式: {len(cities)} 个弱城市需要重试")
    else:
        cities = [c for c in cities if start <= c['rank'] <= end]

    # 有budget_url优先
    with_url = [c for c in cities if c.get('budget_url')]
    without_url = [c for c in cities if not c.get('budget_url')]
    ordered = with_url + without_url

    logger.info(f"开始爬取 {len(ordered)} 个城市, {len(TARGET_DEPTS)}个目标部门")
    logger.info(f"有URL: {len(with_url)}, 需探测: {len(without_url)}")

    total_found = 0
    total_downloaded = 0

    for i, city_info in enumerate(ordered):
        try:
            logger.info(f"--- 进度: {i+1}/{len(ordered)} ---")
            result = process_city(city_info, progress, force=force)
            if result and isinstance(result, dict):
                total_found += result.get('found', 0)
                total_downloaded += result.get('downloaded', 0)
        except Exception as e:
            logger.error(f"[{city_info['city']}] 异常: {e}")
            logger.error(traceback.format_exc())

        time.sleep(DELAY_CITY)

    save_progress(progress)

    # 输出总结
    completed = progress.get('completed', {})
    good = sum(1 for v in completed.values() if isinstance(v, dict) and v.get('found', 0) >= MIN_DEPT_COVERAGE)
    total = len(completed)
    logger.info("=" * 60)
    logger.info(f"完成! 找到 {total_found} 部门, 下载 {total_downloaded} 文件")
    logger.info(f"城市覆盖: {good}/{total} 达标(>={MIN_DEPT_COVERAGE}部门)")
    logger.info("=" * 60)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--start', type=int, default=1)
    p.add_argument('--end', type=int, default=100)
    p.add_argument('--city', type=str, default=None)
    p.add_argument('--force', action='store_true', help='强制重新爬取')
    p.add_argument('--retry-weak', action='store_true', help='只重试弱城市')
    args = p.parse_args()

    if args.city:
        with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
            cities = json.load(f)
        ci = next((c for c in cities if c['city'] == args.city), None)
        if ci:
            progress = load_progress()
            process_city(ci, progress, force=args.force)
        else:
            print(f"未找到: {args.city}")
    else:
        run(args.start, args.end, force=args.force, retry_weak=args.retry_weak)
