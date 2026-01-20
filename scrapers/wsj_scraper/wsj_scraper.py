import os
import json
import yaml
import time
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ========================
# Logging
# ========================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

# ========================
# Paths
# ========================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
COOKIES_PATH = os.path.join(os.path.dirname(__file__), "cookies.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "wsj_raw.json")

# ========================
# Load config
# ========================


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["wsj"]


# ========================
# Load cookies
# ========================


def load_cookies(context):
    if not os.path.exists(COOKIES_PATH):
        raise FileNotFoundError("cookies.json não encontrado")

    with open(COOKIES_PATH, "r", encoding="utf-8") as f:
        cookies = json.load(f)

    for c in cookies:
        if "sameSite" in c and c["sameSite"] not in ["Strict", "Lax", "None"]:
            c["sameSite"] = "Lax"

    context.add_cookies(cookies)


# ========================
# Scraper
# ========================


def run_wsj_scraper():
    cfg = load_config()

    SEARCH_URL = cfg["search_url"]
    MAX_ARTICLES = cfg.get("max_articles", 20)
    TIMEOUT = cfg.get("timeout", 30000)
    COUNTRY = cfg.get("country", "USA")
    SOURCE = cfg.get("source", "The Wall Street Journal")

    articles = []

    logging.info("Starting WSJ scraper with Playwright + cookies...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        load_cookies(context)

        page = context.new_page()
        page.set_default_timeout(20000)
        page.set_default_navigation_timeout(20000)

        try:
            logging.info("Opening WSJ search page...")
            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=TIMEOUT)

            # Espera APENAS o primeiro artigo
            page.wait_for_selector("a[data-testid='flexcard-headline']", timeout=15000)

        except PlaywrightTimeoutError:
            logging.error("Timeout ao aguardar os artigos do WSJ.")
            browser.close()
            return []

        logging.info("Extracting articles...")

        cards = page.query_selector_all("a[data-testid='flexcard-headline']")
        logging.info(f"Found {len(cards)} article cards")

        for card in cards[:MAX_ARTICLES]:
            try:
                title = card.inner_text().strip()
                url = card.get_attribute("href")

                parent = card.evaluate_handle("el => el.closest('div')")
                page.wait_for_timeout(50)

                # Snippet
                snippet_el = parent.query_selector("p[data-testid='flexcard-text']")
                snippet = snippet_el.inner_text().strip() if snippet_el else None

                # Timestamp
                time_el = parent.query_selector("p[data-testid='timestamp-text']")
                published_date = time_el.inner_text().strip() if time_el else None

                articles.append(
                    {
                        "source": SOURCE,
                        "country": COUNTRY,
                        "title": title,
                        "url": url,
                        "published_date": published_date,
                        "summary": snippet,
                        "author": None,
                        "scraped_at": datetime.utcnow().isoformat(),
                    }
                )

            except Exception as e:
                logging.warning(f"Error parsing article: {e}")
                continue

        browser.close()

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    logging.info(f"WSJ scraping finished. Total articles: {len(articles)}")
    return articles


# ========================
# Entry point
# ========================

if __name__ == "__main__":
    run_wsj_scraper()
