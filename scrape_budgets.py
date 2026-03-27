#!/usr/bin/env python3
"""
中国百强城市2026年部门预算爬取脚本
从各城市政府官网爬取指定委办局的2026年预算文件（PDF/HTML）
"""

import json
import os
import re
import time
import logging
import hashlib
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ========== 配置 ==========
BASE_DIR = Path(__file__).parent / "预算数据"
LOG_FILE = Path(__file__).parent / "scrape.log"
PROGRESS_FILE = Path(__file__).parent / "scrape_progress.json"
CITY_DATA_FILE = Path(__file__).parent / "city_data.json"

# 请求配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}
TIMEOUT = 30
RETRY_COUNT = 3
DELAY_BETWEEN_REQUESTS = 1  # 秒，避免被封
DELAY_BETWEEN_CITIES = 3    # 城市间延迟

# 需要爬取的委办局清单
TARGET_DEPARTMENTS = [
    "卫生健康委员会",
    "教育局",
    "发展和改革局",
    "规划和自然资源局",
    "交通运输局",
    "科技创新局",
    "水务局",
    "人力资源和社会保障局",
    "工业和信息化局",
    "市场监督管理局",
    "国有资产监督管理委员会",
    "公安局",
    "公安局交通警察局",
    "医疗保障局",
    "商务局",
    "文化广电旅游体育局",
    "生态环境局",
    "政务服务和数据管理局",
    "城市管理和综合执法局",
    "退役军人事务局",
    "中共深圳市区委宣传部",
    "司法局",
    "住房和建设局",
    "建筑工务署",
    "民政局",
    "财政局",
    "气象局",
    "应急管理局",
    "审计局",
    "政府办公厅",
    "统计局",
    "信访局",
]

# 不同城市可能使用的部门名称变体映射
# key = 标准名称, value = 可能出现的变体列表
DEPT_ALIASES = {
    "卫生健康委员会": ["卫生健康委", "卫健委", "卫生健康局", "卫生局", "卫生和健康委员会", "卫生健康委员会"],
    "教育局": ["教育局", "教育委员会", "教委", "市教育局"],
    "发展和改革局": ["发展和改革局", "发展改革局", "发改局", "发展和改革委员会", "发改委", "发展改革委"],
    "规划和自然资源局": ["规划和自然资源局", "自然资源和规划局", "自然资源局", "规划局", "国土资源局", "规划与自然资源局", "自然资源与规划局"],
    "交通运输局": ["交通运输局", "交通局", "交通委员会", "交通委", "交通运输委员会"],
    "科技创新局": ["科技创新局", "科学技术局", "科技局", "科技创新委员会", "科技创新委"],
    "水务局": ["水务局", "水利局", "水利水务局"],
    "人力资源和社会保障局": ["人力资源和社会保障局", "人社局", "人力资源社会保障局", "人力资源局"],
    "工业和信息化局": ["工业和信息化局", "工信局", "经济和信息化局", "经济和信息化委员会", "经信局", "经信委", "工业信息化局"],
    "市场监督管理局": ["市场监督管理局", "市场监管局", "市场监管委"],
    "国有资产监督管理委员会": ["国有资产监督管理委员会", "国资委", "国有资产监管委"],
    "公安局": ["公安局", "市公安局"],
    "公安局交通警察局": ["公安局交通警察局", "交警局", "公安交通管理局", "交通警察局", "交警支队"],
    "医疗保障局": ["医疗保障局", "医保局"],
    "商务局": ["商务局", "商务委员会", "商务委"],
    "文化广电旅游体育局": ["文化广电旅游体育局", "文化广电旅游局", "文化和旅游局", "文旅局", "文化旅游局", "文化局", "文广旅体局", "文化体育旅游局"],
    "生态环境局": ["生态环境局", "环境保护局", "环保局"],
    "政务服务和数据管理局": ["政务服务和数据管理局", "政务服务局", "政务数据局", "大数据管理局", "政务服务数据管理局", "数据局", "行政审批局"],
    "城市管理和综合执法局": ["城市管理和综合执法局", "城市管理局", "城管局", "城管执法局", "综合行政执法局", "城市管理综合执法局"],
    "退役军人事务局": ["退役军人事务局", "退役军人局"],
    "中共深圳市区委宣传部": ["宣传部", "委宣传部", "区委宣传部", "市委宣传部"],
    "司法局": ["司法局"],
    "住房和建设局": ["住房和建设局", "住房和城乡建设局", "住建局", "住房建设局", "住房城乡建设局"],
    "建筑工务署": ["建筑工务署", "建设工程管理中心", "建设工程事务署", "建筑工务中心"],
    "民政局": ["民政局"],
    "财政局": ["财政局"],
    "气象局": ["气象局", "气象台"],
    "应急管理局": ["应急管理局", "应急局", "安全生产监督管理局"],
    "审计局": ["审计局"],
    "政府办公厅": ["政府办公厅", "政府办公室", "办公厅", "办公室", "人民政府办公厅", "人民政府办公室"],
    "统计局": ["统计局"],
    "信访局": ["信访局", "信访办", "群众信访局"],
}

# ========== 日志设置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BudgetScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.verify = False
        self.progress = self.load_progress()

    def load_progress(self):
        """加载进度文件，支持断点续爬"""
        if PROGRESS_FILE.exists():
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"completed_cities": [], "failed_cities": [], "city_details": {}}

    def save_progress(self):
        """保存进度"""
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=2)

    def fetch_page(self, url, encoding=None):
        """获取页面内容，带重试"""
        for attempt in range(RETRY_COUNT):
            try:
                resp = self.session.get(url, timeout=TIMEOUT, allow_redirects=True)
                if encoding:
                    resp.encoding = encoding
                else:
                    resp.encoding = resp.apparent_encoding or 'utf-8'
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/{RETRY_COUNT} failed for {url}: {e}")
                if attempt < RETRY_COUNT - 1:
                    time.sleep(2 ** attempt)
        return None

    def download_file(self, url, save_path):
        """下载文件（PDF等）"""
        for attempt in range(RETRY_COUNT):
            try:
                resp = self.session.get(url, timeout=60, stream=True)
                if resp.status_code == 200:
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    with open(save_path, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logger.info(f"下载成功: {save_path}")
                    return True
                logger.warning(f"下载失败 HTTP {resp.status_code}: {url}")
            except Exception as e:
                logger.warning(f"下载尝试 {attempt+1} 失败: {url}: {e}")
                if attempt < RETRY_COUNT - 1:
                    time.sleep(2 ** attempt)
        return False

    def save_html_content(self, content, save_path):
        """保存HTML内容"""
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"保存HTML: {save_path}")

    def match_department(self, text, city_name=""):
        """
        将页面上找到的部门名称匹配到目标部门
        返回匹配到的标准部门名称，未匹配返回None
        """
        if not text:
            return None
        # 清理文本
        text = text.strip()
        # 移除城市名前缀 (如 "深圳市卫生健康委员会" -> "卫生健康委员会")
        for prefix in [city_name, city_name + "市", "市"]:
            if prefix and text.startswith(prefix):
                text = text[len(prefix):]

        for dept_name, aliases in DEPT_ALIASES.items():
            for alias in aliases:
                if alias in text or text in alias:
                    # 特殊处理：公安局 vs 公安局交通警察局
                    if dept_name == "公安局" and ("交通" in text or "交警" in text):
                        return "公安局交通警察局"
                    if dept_name == "公安局交通警察局" and ("交通" not in text and "交警" not in text):
                        continue
                    return dept_name
        return None

    def find_budget_links_on_page(self, url, city_name):
        """
        在预算公开页面上查找各部门预算链接
        返回 {部门标准名: [(链接文字, 完整URL, 文件类型), ...]}
        """
        resp = self.fetch_page(url)
        if not resp:
            logger.error(f"无法访问预算页面: {url}")
            return {}

        soup = BeautifulSoup(resp.text, 'lxml')
        found = {}

        # 查找所有链接
        for a_tag in soup.find_all('a', href=True):
            link_text = a_tag.get_text(strip=True)
            href = a_tag['href']

            if not link_text or len(link_text) < 2:
                continue

            # 尝试匹配部门
            dept = self.match_department(link_text, city_name)
            if not dept:
                continue

            # 检查是否包含预算关键词（有些页面链接直接就是部门名称）
            full_url = urljoin(url, href)
            file_type = 'pdf' if href.lower().endswith('.pdf') else 'html'

            if dept not in found:
                found[dept] = []
            found[dept].append((link_text, full_url, file_type))

        # 如果直接找不到，尝试搜索包含"2026"和"预算"的链接
        if not found:
            for a_tag in soup.find_all('a', href=True):
                link_text = a_tag.get_text(strip=True)
                href = a_tag['href']
                if ('2026' in link_text or '预算' in link_text) and link_text:
                    dept = self.match_department(link_text, city_name)
                    if dept:
                        full_url = urljoin(url, href)
                        file_type = 'pdf' if href.lower().endswith('.pdf') else 'html'
                        if dept not in found:
                            found[dept] = []
                        found[dept].append((link_text, full_url, file_type))

        return found

    def scrape_sub_page_for_pdfs(self, url, dept_name, city_folder):
        """
        进入部门预算子页面，查找并下载PDF文件
        """
        resp = self.fetch_page(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, 'lxml')
        downloaded = []

        # 查找所有PDF链接
        pdf_links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            text = a_tag.get_text(strip=True)
            if href.lower().endswith('.pdf') or 'pdf' in href.lower():
                full_url = urljoin(url, href)
                pdf_links.append((text or 'document', full_url))
            elif '预算' in text and ('2026' in text or '2026' in href):
                full_url = urljoin(url, href)
                pdf_links.append((text, full_url))

        # 如果没有PDF链接，保存页面本身作为HTML
        if not pdf_links:
            safe_name = re.sub(r'[\\/:*?"<>|]', '_', dept_name)
            save_path = os.path.join(city_folder, f"{safe_name}_2026年预算.html")
            self.save_html_content(resp.text, save_path)
            downloaded.append(save_path)
        else:
            for text, pdf_url in pdf_links:
                safe_name = re.sub(r'[\\/:*?"<>|]', '_', dept_name)
                safe_text = re.sub(r'[\\/:*?"<>|]', '_', text)[:50]
                filename = f"{safe_name}_{safe_text}.pdf"
                save_path = os.path.join(city_folder, filename)
                if self.download_file(pdf_url, save_path):
                    downloaded.append(save_path)
                time.sleep(DELAY_BETWEEN_REQUESTS)

        return downloaded

    def try_common_budget_paths(self, website, city_name):
        """
        对于没有提供预算URL的城市，尝试常见的预算公开路径
        """
        common_paths = [
            "/zwgk/zdly/czxx/",
            "/zwgk/czzj/",
            "/zfxxgk/fdzdgknr/czxx/",
            "/zfxxgk/czxx/",
            "/zwgk/czgk/",
            "/col/col_budget/",
            "/zwgk/bmys/",
            "/xxgk/czxx/",
            "/zwgk/zdly/czzj/bmczyjs/",
            "/zfxxgk/fdzdgknr/ysjs/",
            "/szf/ztzl/ysgk/",
            "/yjsgk/",
        ]

        # 也尝试搜索
        search_paths = [
            f"/site/search?searchWord=2026+预算",
            f"/search?q=2026+部门预算",
        ]

        for path in common_paths:
            url = urljoin(website, path)
            resp = self.fetch_page(url)
            if resp and resp.status_code == 200 and len(resp.text) > 1000:
                # 检查页面是否包含预算相关内容
                if '预算' in resp.text and ('2026' in resp.text or '部门' in resp.text):
                    logger.info(f"[{city_name}] 发现预算页面: {url}")
                    return url
            time.sleep(0.5)

        return None

    def process_city(self, city_info):
        """处理单个城市"""
        rank = city_info['rank']
        city = city_info['city']
        website = city_info['website']
        budget_url = city_info.get('budget_url')

        city_key = f"{rank:03d}_{city}"
        city_folder = str(BASE_DIR / city_key)
        os.makedirs(city_folder, exist_ok=True)

        # 检查是否已完成
        if city_key in self.progress.get('completed_cities', []):
            logger.info(f"[{city}] 已完成，跳过")
            return

        logger.info(f"{'='*60}")
        logger.info(f"开始处理: [{rank}] {city} - {website}")
        logger.info(f"{'='*60}")

        city_result = {
            "found_departments": [],
            "missing_departments": [],
            "downloaded_files": [],
            "errors": []
        }

        # 如果没有预算URL，尝试查找
        if not budget_url:
            logger.info(f"[{city}] 无预算URL，尝试探测...")
            budget_url = self.try_common_budget_paths(website, city)
            if not budget_url:
                logger.warning(f"[{city}] 未找到预算页面，尝试使用官网首页查找")
                budget_url = website

        # 从预算页面查找各部门链接
        logger.info(f"[{city}] 访问预算页面: {budget_url}")
        dept_links = self.find_budget_links_on_page(budget_url, city)

        # 有些城市的预算页面可能需要翻页或进入子分类
        if len(dept_links) < 5:
            # 尝试在预算页面中找到子页面链接
            resp = self.fetch_page(budget_url)
            if resp:
                soup = BeautifulSoup(resp.text, 'lxml')
                for a_tag in soup.find_all('a', href=True):
                    text = a_tag.get_text(strip=True)
                    href = a_tag['href']
                    if any(kw in text for kw in ['部门预算', '2026', '预算公开', '部门']) and href:
                        sub_url = urljoin(budget_url, href)
                        if sub_url != budget_url:
                            logger.info(f"[{city}] 探索子页面: {text} -> {sub_url}")
                            sub_links = self.find_budget_links_on_page(sub_url, city)
                            for dept, links in sub_links.items():
                                if dept not in dept_links:
                                    dept_links[dept] = links
                            time.sleep(DELAY_BETWEEN_REQUESTS)

        # 下载找到的部门预算文件
        for dept_name, links in dept_links.items():
            logger.info(f"[{city}] 找到部门: {dept_name} ({len(links)}个链接)")
            city_result['found_departments'].append(dept_name)

            for link_text, link_url, file_type in links:
                if file_type == 'pdf':
                    safe_name = re.sub(r'[\\/:*?"<>|]', '_', dept_name)
                    safe_text = re.sub(r'[\\/:*?"<>|]', '_', link_text)[:50]
                    save_path = os.path.join(city_folder, f"{safe_name}_{safe_text}.pdf")
                    if self.download_file(link_url, save_path):
                        city_result['downloaded_files'].append(save_path)
                else:
                    # 进入子页面找PDF
                    downloaded = self.scrape_sub_page_for_pdfs(link_url, dept_name, city_folder)
                    city_result['downloaded_files'].extend(downloaded)
                time.sleep(DELAY_BETWEEN_REQUESTS)

        # 记录未找到的部门
        for dept in TARGET_DEPARTMENTS:
            if dept not in city_result['found_departments']:
                city_result['missing_departments'].append(dept)

        # 生成该城市的汇总文件
        summary_path = os.path.join(city_folder, "爬取汇总.txt")
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"城市: {city} (排名: {rank})\n")
            f.write(f"官网: {website}\n")
            f.write(f"预算页面: {budget_url}\n")
            f.write(f"爬取时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"\n已找到的部门 ({len(city_result['found_departments'])}):\n")
            for d in city_result['found_departments']:
                f.write(f"  ✓ {d}\n")
            f.write(f"\n未找到的部门 ({len(city_result['missing_departments'])}):\n")
            for d in city_result['missing_departments']:
                f.write(f"  ✗ {d}\n")
            f.write(f"\n下载的文件 ({len(city_result['downloaded_files'])}):\n")
            for fp in city_result['downloaded_files']:
                f.write(f"  - {os.path.basename(fp)}\n")

        # 保存进度
        self.progress['city_details'][city_key] = city_result
        if len(city_result['found_departments']) > 0:
            self.progress['completed_cities'].append(city_key)
        else:
            self.progress['failed_cities'].append(city_key)
        self.save_progress()

        logger.info(f"[{city}] 完成 - 找到{len(city_result['found_departments'])}个部门, "
                    f"下载{len(city_result['downloaded_files'])}个文件")

    def run(self, start_rank=1, end_rank=100):
        """运行爬取"""
        with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
            cities = json.load(f)

        # 过滤范围
        cities = [c for c in cities if start_rank <= c['rank'] <= end_rank]

        logger.info(f"开始爬取 {len(cities)} 个城市的部门预算")
        logger.info(f"目标部门: {len(TARGET_DEPARTMENTS)} 个")

        for city_info in cities:
            try:
                self.process_city(city_info)
            except Exception as e:
                logger.error(f"处理 {city_info['city']} 时出错: {e}", exc_info=True)
                self.progress['failed_cities'].append(f"{city_info['rank']:03d}_{city_info['city']}")
                self.save_progress()

            time.sleep(DELAY_BETWEEN_CITIES)

        # 最终汇总
        self.print_summary()

    def print_summary(self):
        """打印最终汇总"""
        logger.info("\n" + "=" * 80)
        logger.info("爬取完成汇总")
        logger.info("=" * 80)
        logger.info(f"成功城市: {len(self.progress.get('completed_cities', []))}")
        logger.info(f"失败城市: {len(self.progress.get('failed_cities', []))}")

        total_files = 0
        total_found = 0
        total_missing = 0
        for city_key, detail in self.progress.get('city_details', {}).items():
            total_files += len(detail.get('downloaded_files', []))
            total_found += len(detail.get('found_departments', []))
            total_missing += len(detail.get('missing_departments', []))

        logger.info(f"总下载文件数: {total_files}")
        logger.info(f"总找到部门数: {total_found}")
        logger.info(f"总未找到部门数: {total_missing}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='中国百强城市2026年部门预算爬取')
    parser.add_argument('--start', type=int, default=1, help='起始排名 (默认1)')
    parser.add_argument('--end', type=int, default=100, help='结束排名 (默认100)')
    parser.add_argument('--city', type=str, help='指定城市名 (如: 深圳)')
    args = parser.parse_args()

    # 禁用SSL警告
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    scraper = BudgetScraper()

    if args.city:
        with open(CITY_DATA_FILE, 'r', encoding='utf-8') as f:
            cities = json.load(f)
        city_info = next((c for c in cities if c['city'] == args.city), None)
        if city_info:
            scraper.process_city(city_info)
        else:
            print(f"未找到城市: {args.city}")
    else:
        scraper.run(start_rank=args.start, end_rank=args.end)
