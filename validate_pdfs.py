#!/usr/bin/env python3
"""
validate_pdfs.py - Validate, rename, and clean up PDF/HTML budget files in 预算数据/ directory.

For each city directory under 预算数据/:
1. Extract the main title from each PDF (via PyMuPDF largest-font text) or HTML (<title>/<h1>)
2. Match the title against 33 target departments using comprehensive variant matching
3. Rename matching files to "委办局名_2026年部门预算.{ext}"
4. Delete sub-unit/subsidiary budget files and non-matching files
5. Log all actions and print a summary
"""

import os
import re
import sys
import logging
from pathlib import Path
from html.parser import HTMLParser

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: PyMuPDF (fitz) is required. Install with: pip install PyMuPDF")
    sys.exit(1)

# ============================================================================
# Configuration
# ============================================================================

BASE_DIR = Path(__file__).parent / "预算数据"
LOG_FILE = Path(__file__).parent / "logs" / "validate.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

SKIP_FILES = {"爬取汇总.txt", ".gitkeep"}

# ============================================================================
# Logging
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ============================================================================
# 33 target departments (canonical names)
# ============================================================================

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

# ============================================================================
# Department variant matching rules
# Order matters: more specific patterns (e.g. 公安局交通警察局) MUST come
# before less specific ones (e.g. 公安局) to avoid premature matching.
# ============================================================================

MATCH_RULES = [
    # --- 公安局交通警察局 (must precede 公安局) ---
    (["公安局交通警察局", "公安局交通警察", "公安交通管理", "交通警察局",
      "交警局", "交警支队", "公安交警"], "公安局交通警察局"),

    # --- 卫生健康委员会 ---
    (["卫生健康委员会", "卫生健康委", "卫健委", "卫生健康局", "卫生局",
      "卫生和健康委", "卫健局", "卫生和计划生育委"], "卫生健康委员会"),

    # --- 教育局 ---
    (["教育局", "教育委员会", "教育委", "教委", "教育体育局", "教体局"], "教育局"),

    # --- 发展和改革局 ---
    (["发展和改革局", "发展改革局", "发展和改革委员会", "发展改革委员会",
      "发展和改革委", "发展改革委", "发改委", "发改局"], "发展和改革局"),

    # --- 规划和自然资源局 ---
    (["规划和自然资源局", "规划与自然资源局", "自然资源和规划局",
      "自然资源与规划局", "自然资源局", "自然资源和城乡规划局",
      "国土资源局", "规划局", "国土局"], "规划和自然资源局"),

    # --- 交通运输局 ---
    (["交通运输局", "交通运输委", "交通局", "交通委员会", "交通委"], "交通运输局"),

    # --- 科技创新局 ---
    (["科技创新局", "科学技术局", "科技局", "科技创新委"], "科技创新局"),

    # --- 水务局 ---
    (["水务局", "水利局", "水利水务局", "水利水务"], "水务局"),

    # --- 人力资源和社会保障局 ---
    (["人力资源和社会保障局", "人力资源社会保障局", "人社局", "人力社保局",
      "人力资源和社会保障委"], "人力资源和社会保障局"),

    # --- 工业和信息化局 ---
    (["工业和信息化局", "工业信息化局", "工信局", "经济和信息化局",
      "经信局", "经信委", "经济和信息化委", "工信委",
      "工业和信息化委"], "工业和信息化局"),

    # --- 市场监督管理局 ---
    (["市场监督管理局", "市场监管局"], "市场监督管理局"),

    # --- 国有资产监督管理委员会 ---
    (["国有资产监督管理委员会", "国有资产监督管理局", "国资委",
      "国有资产监管委"], "国有资产监督管理委员会"),

    # --- 公安局 (after 公安局交通警察局) ---
    (["公安局", "市公安局"], "公安局"),

    # --- 医疗保障局 ---
    (["医疗保障局", "医保局", "医疗保障委"], "医疗保障局"),

    # --- 商务局 ---
    (["商务局", "商务委", "商务厅"], "商务局"),

    # --- 文化广电旅游体育局 ---
    (["文化广电旅游体育局", "文化广电旅游体育", "文化广电旅游局",
      "文化广电旅游", "文化和旅游局", "文旅局", "文广旅体局",
      "文广旅体", "文化体育和旅游局", "文化体育旅游局", "文化旅游广电",
      "文体旅游局", "文广旅局", "文广新旅局", "文广新旅",
      "文化广电和旅游局", "文体广旅局", "文体广电旅游局",
      "文化旅游局"], "文化广电旅游体育局"),

    # --- 生态环境局 ---
    (["生态环境局", "环境保护局", "环保局", "生态环境厅"], "生态环境局"),

    # --- 政务服务和数据管理局 ---
    (["政务服务和数据管理局", "政务服务与数据管理局", "政务服务数据管理局",
      "政务数据局", "政务服务局", "政务服务管理局", "大数据管理局",
      "大数据发展局", "数据管理局", "政数局", "数据局",
      "行政审批局", "政务和大数据局",
      "行政审批服务局"], "政务服务和数据管理局"),

    # --- 城市管理和综合执法局 ---
    (["城市管理和综合执法局", "城市管理综合执法局", "城市管理和综合执法",
      "城市管理综合执法", "城市管理局", "城管局", "城管执法局",
      "综合行政执法局", "综合执法局", "城管和综合执法局",
      "城市管理行政执法局", "城市管理委员会"], "城市管理和综合执法局"),

    # --- 退役军人事务局 ---
    (["退役军人事务局", "退役军人局", "退役军人事务"], "退役军人事务局"),

    # --- 宣传部 ---
    (["宣传部", "市委宣传部"], "宣传部"),

    # --- 司法局 ---
    (["司法局"], "司法局"),

    # --- 住房和建设局 ---
    (["住房和建设局", "住房建设局", "住房和城乡建设局", "住房城乡建设局",
      "住建局", "住房保障和房屋管理局", "住房保障和房管局",
      "住建委", "住房和城乡建设委"], "住房和建设局"),

    # --- 建筑工务署 ---
    (["建筑工务署", "工务署", "建设工务署", "建筑工务中心",
      "建设工程事务署", "建设工务"], "建筑工务署"),

    # --- 民政局 ---
    (["民政局"], "民政局"),

    # --- 财政局 ---
    (["财政局", "财政委"], "财政局"),

    # --- 气象局 ---
    (["气象局"], "气象局"),

    # --- 应急管理局 ---
    (["应急管理局", "应急局", "安全生产监督管理局", "安监局",
      "应急管理委"], "应急管理局"),

    # --- 审计局 ---
    (["审计局"], "审计局"),

    # --- 政府办公厅 ---
    (["政府办公厅", "政府办公室", "人民政府办公室", "人民政府办公厅",
      "市政府办公室", "市政府办公厅"], "政府办公厅"),

    # --- 统计局 ---
    (["统计局"], "统计局"),

    # --- 信访局 ---
    (["信访局", "信访办公室", "信访办", "信访事务中心",
      "群众来访接待"], "信访局"),

    # --- 口岸办公室 ---
    (["口岸办公室", "口岸办", "口岸事务", "口岸局", "口岸管理局"], "口岸办公室"),
]

# ============================================================================
# Sub-unit keywords: files whose title contains these should be DELETED,
# unless the keyword is part of a recognized department variant name or
# the title contains "本级".
# ============================================================================

SUB_UNIT_KEYWORDS = [
    "分局", "监管局",
    "服务中心", "事务中心", "管理所",  # multi-char first (before "中心")
    "中心", "研究院", "研究所", "学校", "学院", "医院",
    "站", "大队", "支队", "总队",
    "监测", "培训", "幼儿园",
    "纪念馆", "博物馆", "图书馆", "文化馆", "美术馆", "展示馆",
    "福利院", "救助", "戒毒", "疗养", "供应站", "试验场",
]

# These strings, if found in the title, suppress the sub-unit check.
# They are department variant names that happen to contain sub-unit keywords.
SUB_UNIT_EXCEPTIONS = [
    "政务服务中心",
    "建筑工务中心",
    "信访事务中心",
    "交警支队",
    "气象台",
    "行政审批服务局",
]

# Build a set of all variant keywords so we can use them to "mask" the title
# before checking for sub-unit keywords.
_ALL_DEPT_VARIANTS: set[str] = set()
for _variants, _canonical in MATCH_RULES:
    _ALL_DEPT_VARIANTS.update(_variants)
    _ALL_DEPT_VARIANTS.add(_canonical)


# ============================================================================
# Sub-unit detection
# ============================================================================

def is_sub_unit(title: str) -> bool:
    """
    Return True if *title* indicates a sub-unit / subsidiary budget file.

    "本级" in the title explicitly means main department budget -> NOT a sub-unit.
    """
    if "本级" in title:
        return False

    # Check exceptions first
    for exc in SUB_UNIT_EXCEPTIONS:
        if exc in title:
            return False

    # Remove all known department variant names from the title so that
    # e.g. "市场监督管理局" does not trigger "监管局".
    masked = title
    for variant in sorted(_ALL_DEPT_VARIANTS, key=len, reverse=True):
        masked = masked.replace(variant, "")

    # Also strip common structural words
    masked = re.sub(r"\d{4}\s*年", "", masked)
    masked = re.sub(r"[省市区县]", "", masked)
    masked = masked.replace("部门预算", "").replace("单位预算", "")
    masked = masked.replace("预算公开", "").replace("预算说明", "")
    masked = masked.replace("公开说明", "")
    masked = masked.replace("汇总", "").replace("年度", "")

    for kw in SUB_UNIT_KEYWORDS:
        if kw in masked:
            return True

    return False


# ============================================================================
# Department matching
# ============================================================================

def match_department(text: str) -> str | None:
    """
    Match *text* against the 33 target departments using MATCH_RULES.
    Returns the canonical department name, or None.
    """
    for variants, canonical in MATCH_RULES:
        for variant in variants:
            if variant in text:
                return canonical
    return None


# ============================================================================
# Title extraction - PDF
# ============================================================================

def extract_pdf_title(filepath: str) -> str | None:
    """
    Open a PDF with PyMuPDF, read the first page, and extract the main title.
    The title is the largest-font text on the first page.
    Returns the title string, or None on failure.
    """
    try:
        doc = fitz.open(filepath)
    except Exception as e:
        logger.warning("  Cannot open PDF %s: %s", filepath, e)
        return None

    try:
        if len(doc) == 0:
            return None

        page = doc[0]
        blocks = page.get_text("dict")["blocks"]

        # Collect (font_size, text) for every span
        spans = []
        for block in blocks:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    text = span["text"].strip()
                    if text:
                        spans.append((span["size"], text))

        if not spans:
            # Fallback to plain text
            text = page.get_text().strip()
            return text[:300] if text else None

        # Find the maximum font size
        max_size = max(s[0] for s in spans)

        # Collect all text within 2pt of the max size (title text)
        title_parts = []
        for size, text in spans:
            if size >= max_size - 2:
                title_parts.append(text)

        title = "".join(title_parts).strip()

        # If title is too short or has no Chinese characters, use first N spans
        if len(title) < 4 or not re.search(r"[\u4e00-\u9fff]{2,}", title):
            all_text = "".join(t for _, t in spans[:20])
            title = all_text[:300]

        return title.strip() if title.strip() else None

    except Exception as e:
        logger.warning("  Error reading PDF %s: %s", filepath, e)
        return None
    finally:
        doc.close()


# ============================================================================
# Title extraction - HTML
# ============================================================================

class _HTMLTitleParser(HTMLParser):
    """Extract <title> and <h1> text from HTML."""

    def __init__(self):
        super().__init__()
        self._tag_stack: list[str] = []
        self.title = ""
        self.h1_texts: list[str] = []
        self.h2_texts: list[str] = []
        self._buf = ""

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in ("title", "h1", "h2"):
            self._tag_stack.append(tag)
            self._buf = ""

    def handle_endtag(self, tag):
        tag = tag.lower()
        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()
            text = self._buf.strip()
            if tag == "title":
                self.title = text
            elif tag == "h1" and text:
                self.h1_texts.append(text)
            elif tag == "h2" and text:
                self.h2_texts.append(text)

    def handle_data(self, data):
        if self._tag_stack:
            self._buf += data


def extract_html_title(filepath: str) -> str | None:
    """
    Read an HTML file and extract a meaningful title from <title>, <h1>, or <h2>.
    Tries multiple encodings for robustness.
    """
    content = None
    for enc in ("utf-8", "gbk", "gb2312", "gb18030", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc, errors="strict") as f:
                content = f.read()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            logger.warning("  Cannot read HTML %s: %s", filepath, e)
            return None

    if not content:
        return None

    parser = _HTMLTitleParser()
    try:
        parser.feed(content)
    except Exception:
        pass

    # Collect candidates; prefer those that mention "预算"
    candidates = []
    if parser.h1_texts:
        candidates.extend(parser.h1_texts)
    if parser.title:
        candidates.append(parser.title)
    if parser.h2_texts:
        candidates.extend(parser.h2_texts)

    for c in candidates:
        if "预算" in c:
            return c

    # Return the first candidate with meaningful Chinese text
    for c in candidates:
        if re.search(r"[\u4e00-\u9fff]{2,}", c):
            return c

    return candidates[0] if candidates else None


# ============================================================================
# File processing
# ============================================================================

def process_file(filepath: Path, city_dir: Path) -> dict:
    """
    Process a single PDF or HTML file. Returns a result dict:
    { "action": "renamed"|"deleted"|"kept"|"error",
      "reason": str, "old_name": str, "new_name": str|None }
    """
    filename = filepath.name
    suffix = filepath.suffix.lower()
    result = {"old_name": filename, "new_name": None, "action": "error", "reason": ""}

    # ---- Extract title ----
    pdf_full_text = ""
    if suffix == ".pdf":
        title = extract_pdf_title(str(filepath))
        # Also extract full first-page text for budget content validation
        try:
            doc = fitz.open(str(filepath))
            if len(doc) > 0:
                pdf_full_text = doc[0].get_text() or ""
            doc.close()
        except Exception:
            pass
    elif suffix in (".html", ".htm"):
        title = extract_html_title(str(filepath))
    else:
        result["action"] = "kept"
        result["reason"] = f"Unsupported file type: {suffix}"
        return result

    # Combine title with filename hint (the scraper already encoded the dept
    # name into the filename, so we use it as a fallback signal)
    fname_hint = filename.split("_")[0] if "_" in filename else ""

    logger.info("  File: %s", filename)
    logger.info("    Title: %s", (title or "(empty)")[:100])

    # ---- HTML content quality check ----
    # Many HTML files are just department homepages or error pages, not budget docs.
    # For HTML files, also check body text for budget-related content.
    if suffix in (".html", ".htm"):
        has_budget_in_title = title and "预算" in title
        has_budget_in_body = False
        try:
            content = None
            for enc in ("utf-8", "gbk", "gb2312", "gb18030", "latin-1"):
                try:
                    with open(filepath, "r", encoding=enc, errors="strict") as f:
                        content = f.read()
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            if content:
                # Check if body text contains budget-related keywords
                has_budget_in_body = ("部门预算" in content or "预算公开" in content
                                     or ("预算" in content and "2026" in content))
        except Exception:
            pass

        if not has_budget_in_title and not has_budget_in_body:
            result["action"] = "deleted"
            result["reason"] = f"HTML has no budget content: {(title or '(empty)')[:60]}"
            try:
                os.remove(filepath)
                logger.info("    DELETE (HTML no budget content): %s", (title or "(empty)")[:60])
            except OSError as e:
                logger.error("    Failed to delete: %s", e)
                result["reason"] = f"Delete failed: {e}"
            return result

    # ---- PDF content quality check ----
    if suffix == ".pdf" and title and pdf_full_text:
        has_budget_in_pdf = ("预算" in (title + pdf_full_text) or "budget" in (title + pdf_full_text).lower())
        if not has_budget_in_pdf:
            result["action"] = "deleted"
            result["reason"] = f"PDF has no budget content: {title[:60]}"
            try:
                os.remove(filepath)
                logger.info("    DELETE (PDF no budget content): %s", title[:60])
            except OSError as e:
                logger.error("    Failed to delete: %s", e)
                result["reason"] = f"Delete failed: {e}"
            return result

    # ---- Handle empty title ----
    if not title:
        # Cannot extract any title. Use the filename hint only.
        dept_from_fname = match_department(fname_hint)
        if dept_from_fname:
            # Trust the filename
            title = fname_hint
            logger.info("    Title empty; using filename hint: %s", fname_hint)
        else:
            result["action"] = "deleted"
            result["reason"] = "Cannot extract title and filename has no department match"
            try:
                os.remove(filepath)
                logger.info("    DELETE (no title, no filename match)")
            except OSError as e:
                logger.error("    Failed to delete: %s", e)
                result["reason"] = f"Delete failed: {e}"
            return result

    # ---- Check sub-unit ----
    if is_sub_unit(title):
        result["action"] = "deleted"
        result["reason"] = f"Sub-unit detected: {title[:80]}"
        try:
            os.remove(filepath)
            logger.info("    DELETE (sub-unit): %s", title[:80])
        except OSError as e:
            logger.error("    Failed to delete: %s", e)
            result["action"] = "error"
            result["reason"] = f"Delete failed: {e}"
        return result

    # ---- Match department ----
    dept = match_department(title)
    if dept is None:
        # Try matching from filename hint
        dept = match_department(fname_hint)
        if dept:
            logger.info("    Matched from filename hint: %s", dept)

    if dept is None:
        result["action"] = "deleted"
        result["reason"] = f"No matching department: {title[:80]}"
        try:
            os.remove(filepath)
            logger.info("    DELETE (no department match): %s", title[:80])
        except OSError as e:
            logger.error("    Failed to delete: %s", e)
            result["action"] = "error"
            result["reason"] = f"Delete failed: {e}"
        return result

    # ---- Determine correct filename ----
    correct_name = f"{dept}_2026年部门预算{suffix}"
    correct_path = city_dir / correct_name

    if filepath.name == correct_name:
        result["action"] = "kept"
        result["reason"] = f"Already correct ({dept})"
        logger.info("    KEEP (already correct): %s", dept)
        return result

    # Handle collision: another file already has the target name
    if correct_path.exists() and correct_path != filepath:
        # Keep the larger file (more likely to be the real budget document)
        existing_size = correct_path.stat().st_size
        new_size = filepath.stat().st_size
        if new_size > existing_size:
            logger.info("    Replacing existing %s (new=%d > old=%d bytes)",
                        correct_name, new_size, existing_size)
            try:
                os.remove(correct_path)
            except OSError as e:
                logger.error("    Failed to remove existing %s: %s", correct_name, e)
                result["action"] = "error"
                result["reason"] = f"Cannot remove existing: {e}"
                return result
        else:
            result["action"] = "deleted"
            result["reason"] = f"Duplicate; existing {correct_name} is same/larger"
            try:
                os.remove(filepath)
                logger.info("    DELETE (duplicate, existing is larger): %s", correct_name)
            except OSError as e:
                logger.error("    Failed to delete duplicate: %s", e)
                result["action"] = "error"
                result["reason"] = f"Delete failed: {e}"
            return result

    # ---- Rename ----
    try:
        filepath.rename(correct_path)
        result["action"] = "renamed"
        result["new_name"] = correct_name
        result["reason"] = f"Matched: {dept}"
        logger.info("    RENAME: %s -> %s", filename, correct_name)
    except OSError as e:
        logger.error("    Failed to rename %s -> %s: %s", filename, correct_name, e)
        result["action"] = "error"
        result["reason"] = f"Rename failed: {e}"

    return result


# ============================================================================
# Main
# ============================================================================

def main():
    if not BASE_DIR.exists():
        logger.error("Directory not found: %s", BASE_DIR)
        sys.exit(1)

    stats = {"total": 0, "renamed": 0, "deleted": 0, "kept": 0, "errors": 0}

    city_dirs = sorted([d for d in BASE_DIR.iterdir() if d.is_dir()])
    logger.info("=" * 60)
    logger.info("Starting validation across %d city directories", len(city_dirs))
    logger.info("=" * 60)

    for city_dir in city_dirs:
        city_name = city_dir.name
        all_files = sorted([f for f in city_dir.iterdir() if f.is_file()])

        budget_files = [
            f for f in all_files
            if f.name not in SKIP_FILES
            and f.suffix.lower() in (".pdf", ".html", ".htm")
        ]

        if not budget_files:
            continue

        logger.info("")
        logger.info("-" * 40)
        logger.info("City: %s (%d files)", city_name, len(budget_files))
        logger.info("-" * 40)

        # Process correctly-named files first so they "claim" the standard
        # filename before duplicates are processed.
        correct_re = re.compile(r"^.+_2026年部门预算\.(pdf|html?)$")
        correctly_named = [f for f in budget_files if correct_re.match(f.name)]
        others = [f for f in budget_files if not correct_re.match(f.name)]

        for filepath in correctly_named + others:
            # File may have been removed by an earlier iteration (e.g. collision)
            if not filepath.exists():
                continue

            stats["total"] += 1
            result = process_file(filepath, city_dir)

            action = result["action"]
            if action == "renamed":
                stats["renamed"] += 1
            elif action == "deleted":
                stats["deleted"] += 1
            elif action == "kept":
                stats["kept"] += 1
            elif action == "error":
                stats["errors"] += 1

    # ---- Summary ----
    summary = f"""
{'=' * 60}
VALIDATION COMPLETE - SUMMARY
{'=' * 60}
Total files checked:   {stats['total']}
  Renamed:             {stats['renamed']}
  Deleted:             {stats['deleted']}
  Kept (already OK):   {stats['kept']}
  Errors:              {stats['errors']}
{'=' * 60}
"""
    logger.info(summary)
    print(summary)


if __name__ == "__main__":
    main()
