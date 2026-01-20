"""
weibo_scraper.py
Version: 2.0 (Scraper only)

Playwright + BeautifulSoup scraper for Weibo search results.

Responsibilities:
- Collect raw Weibo posts
- Handle authentication via cookies
- Stop automatically when no new posts are found
- Output ONLY raw JSON for downstream parsers / API

Output:
- output/weibo_raw.json
"""

import os
import time
import json
import yaml
import re
import datetime
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ========================
# Paths
# ========================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
COOKIES_PATH = os.path.join(os.path.dirname(__file__), "cookies.json")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
RAW_JSON = os.path.join(OUTPUT_DIR, "weibo_raw.json")


# ========================
# Utilities
# ========================


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def clean_text(html):
    soup = BeautifulSoup(html or "", "lxml")
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text)


def safe_int(text):
    if not text:
        return 0
    text = str(text).replace(",", "").strip()
    m = re.search(r"([\d.]+)\s*万", text)
    if m:
        return int(float(m.group(1)) * 10000)
    m2 = re.search(r"\d+", text)
    return int(m2.group()) if m2 else 0


def build_timescope():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    return f"{yesterday}-0:{today}-0"


def build_search_url(query, timescope, page):
    q = quote_plus(query)
    return (
        f"https://s.weibo.com/weibo?"
        f"q={q}&xsort=hot&suball=1"
        f"&timescope=custom:{timescope}"
        f"&page={page}&Refer=g"
    )


def load_cookies(context):
    if not os.path.exists(COOKIES_PATH):
        print("[WARN] cookies.json not found.")
        return False

    with open(COOKIES_PATH, "r", encoding="utf-8") as f:
        cookies = json.load(f)

    playwright_cookies = []
    for c in cookies:
        cookie = {
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain") or c.get("host"),
            "path": c.get("path", "/"),
            "httpOnly": c.get("httpOnly", False),
            "secure": c.get("secure", False),
        }
        if c.get("expiry"):
            cookie["expires"] = int(c["expiry"])
        elif c.get("expirationDate"):
            cookie["expires"] = int(c["expirationDate"])

        playwright_cookies.append(cookie)

    context.add_cookies(playwright_cookies)
    return True


# ========================
# Parsing logic (RAW)
# ========================


def parse_posts(html):
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select('div.card-wrap[action-type="feed_list_item"]')

    posts = []

    for card in cards:
        content_node = card.select_one('p[node-type="feed_list_content"], p.txt')
        if not content_node:
            continue

        raw_html = str(content_node)
        text = clean_text(raw_html)

        if len(text) < 10:
            continue

        mid = card.get("mid") or card.get("data-mid")

        user_el = card.select_one("a.name")
        user_name = user_el.get_text(strip=True) if user_el else None
        user_url = (
            f"https:{user_el['href']}"
            if user_el and user_el.get("href", "").startswith("//")
            else user_el.get("href") if user_el else None
        )

        time_el = card.select_one("div.from a")
        timestamp = time_el.get_text(strip=True) if time_el else None

        region_el = card.select_one(".region_name")
        region = region_el.get_text(strip=True) if region_el else None

        like_el = card.select_one(".woo-like-count")
        likes = safe_int(like_el.get_text(strip=True) if like_el else None)

        posts.append(
            {
                "mid": mid,
                "text": text,
                "raw_html": raw_html,
                "user_name": user_name,
                "user_url": user_url,
                "timestamp": timestamp,
                "region": region,
                "likes": likes,
                "scraped_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
        )

    return posts


# ========================
# Main scraper
# ========================


def run_scraper():
    cfg = load_config()
    ensure_output_dir()

    keyword = cfg.get("keyword", "人工智能")
    posts_limit = int(cfg.get("posts_limit", 200))
    timeout = int(cfg.get("timeout", 30000))
    max_pages = int(cfg.get("max_pages", 40))
    scroll_pause = float(cfg.get("scroll_pause", 1.5))

    timescope = build_timescope()

    all_posts = []
    seen_keys = set()

    no_new_pages = 0
    MAX_NO_NEW_PAGES = 2

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
            )
        )

        load_cookies(context)
        page = context.new_page()

        page_num = 1
        while page_num <= max_pages and len(all_posts) < posts_limit:
            url = build_search_url(keyword, timescope, page_num)
            print(f"[NAV] Page {page_num}")

            try:
                page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            except:
                break

            time.sleep(scroll_pause)
            html = page.content()
            page_posts = parse_posts(html)

            new_count = 0
            for post in page_posts:
                key = (post.get("mid"), post["text"][:120])
                if key in seen_keys:
                    continue

                seen_keys.add(key)
                all_posts.append(post)
                new_count += 1

                if len(all_posts) >= posts_limit:
                    break

            print(
                f"[INFO] Found {len(page_posts)} posts, "
                f"{new_count} new (total {len(all_posts)})"
            )

            if new_count == 0:
                no_new_pages += 1
                if no_new_pages >= MAX_NO_NEW_PAGES:
                    print("[STOP] No new posts detected. Ending scrape.")
                    break
            else:
                no_new_pages = 0

            page_num += 1
            time.sleep(1)

        browser.close()

    with open(RAW_JSON, "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2)

    print(f"[DONE] Saved {len(all_posts)} posts -> {RAW_JSON}")


# ========================
# Entry point
# ========================

if __name__ == "__main__":
    run_scraper()
