"""
peoples_daily_scraper.py
Version: 1.0

Scrapes People's Daily (人民日报) search results about Artificial Intelligence
using Playwright + cookies.

Output:
- output/peoples_daily_raw.json
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
import yaml
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ========================
# Logging
# ========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ========================
# Paths
# ========================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "peoples_daily_raw.json")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
COOKIES_PATH = os.path.join(os.path.dirname(__file__), "cookies.json")


# ========================
# Helpers
# ========================


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["peoples_daily"]


def load_cookies(context):
    if not os.path.exists(COOKIES_PATH):
        logging.warning("Cookies file not found. Continuing without cookies.")
        return

    with open(COOKIES_PATH, "r", encoding="utf-8") as f:
        cookies = json.load(f)

    cleaned_cookies = []

    for c in cookies:
        cookie = c.copy()

        # Corrige sameSite apenas se for string válida
        same_site = cookie.get("sameSite")

        if isinstance(same_site, str):
            same_site = same_site.capitalize()
            if same_site in ("Strict", "Lax", "None"):
                cookie["sameSite"] = same_site
            else:
                cookie.pop("sameSite", None)
        else:
            cookie.pop("sameSite", None)

        cleaned_cookies.append(cookie)

    context.add_cookies(cleaned_cookies)
    logging.info("Cookies loaded successfully.")


# ========================
# Scraper
# ========================


def run_peoples_daily_scraper():
    cfg = load_config()

    logging.info("Starting People's Daily scraper with Playwright + cookies...")

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="zh-CN")
        load_cookies(context)

        page = context.new_page()

        try:
            logging.info("Opening People's Daily search page...")
            page.goto(cfg["search_url"], timeout=cfg["timeout"])

            # Espera apenas o container dos artigos (não o load completo da página)
            page.wait_for_selector("div.sreach_li", timeout=cfg["timeout"])

        except PlaywrightTimeoutError:
            logging.error("Timeout ao carregar a página do People's Daily.")
            browser.close()
            return []

        logging.info("Extracting articles...")
        cards = page.query_selector_all("div.sreach_li")
        logging.info(f"Found {len(cards)} article cards")

        for card in cards[: cfg["max_articles"]]:
            try:
                title_el = card.query_selector("h3 a.open_detail_link")
                title = title_el.inner_text().strip()
                href = title_el.get_attribute("href")

                url = (
                    href
                    if href.startswith("http")
                    else f"https://data.people.com.cn{href}"
                )

                date_el = card.query_selector("div.listinfo")
                published_date = date_el.inner_text().strip() if date_el else None

                summary_el = card.query_selector("div.incon_text p")
                summary = summary_el.inner_text().strip() if summary_el else None

                results.append(
                    {
                        "source": cfg["source"],
                        "country": cfg["country"],
                        "title": title,
                        "url": url,
                        "published_date": published_date,
                        "summary": summary,
                        "author": None,
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            except Exception as e:
                logging.warning(f"Failed to parse one article: {e}")
                continue

        browser.close()

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logging.info(f"People's Daily scraping finished. Total articles: {len(results)}")

    return results


# ========================
# Entry point
# ========================

if __name__ == "__main__":
    run_peoples_daily_scraper()
