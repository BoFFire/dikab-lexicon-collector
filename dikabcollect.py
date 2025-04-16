import time
import random
import csv
import threading
from math import ceil
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm  # for progress bar

# Configuration
BASE_URL = "https://www.dictionnaire-kabyle.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    )
}

# Thread‑local storage for per‑thread Session
thread_local = threading.local()

def create_session_with_retries(
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    allowed_methods: tuple = ("HEAD", "GET", "OPTIONS")
) -> requests.Session:
    """Return a Session that retries on specified HTTP status codes with exponential backoff."""
    session = requests.Session()
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session

def get_session() -> requests.Session:
    """Get or create a thread‑local Session."""
    if not hasattr(thread_local, "session"):
        thread_local.session = create_session_with_retries()
    return thread_local.session

def get_categories() -> list:
    """Extract all categories from the main page."""
    resp = get_session().get(f"{BASE_URL}/category/isem-amalay-asuf/")
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    cats = []
    for section in soup.select("#column-right .shadow-box"):
        for item in section.select(".list-group-item"):
            link = item.select_one("a")
            badge = item.select_one(".badge")
            if not link or not badge:
                continue
            cats.append({
                "name": link.text.strip(),
                "url_path": urlparse(link["href"]).path,
                "count": int(badge.text.strip())
            })
    return cats

def scrape_page(soup: BeautifulSoup, category_name: str) -> list:
    """Parse all word entries from a single category page."""
    results = []
    for lemma in soup.select(".lemma"):
        word_tag = lemma.select_one(".word h2 a")
        gram = lemma.select_one(".w_category")
        trans_tag = lemma.select_one(".w_transcription")
        translations = {"french": "", "english": "", "arabic": ""}
        for feat in lemma.select(".translation .feature"):
            img = feat.select_one("img")
            if img and img.has_attr("title"):
                lang = img["title"].lower()
                txt = feat.select_one("p").text.strip()
                if lang in translations:
                    translations[lang] = txt
        results.append({
            "dictionary_category": category_name,
            "grammatical_category": gram.text.strip() if gram else "",
            "word": word_tag.text.strip() if word_tag else "",
            "transcription": trans_tag.text.strip("[]") if trans_tag else "",
            **translations
        })
    return results

def fetch_and_parse(url: str, category_name: str) -> list:
    """Fetch a URL and return parsed word entries."""
    session = get_session()
    resp = session.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    return scrape_page(soup, category_name)

def main():
    # 1. Get categories and let user choose
    categories = get_categories()
    for i, cat in enumerate(categories, 1):
        print(f"{i}. {cat['name']} ({cat['count']} entries)")
    print("A. All Categories")
    choice = input("Enter number or 'A': ").strip().upper()
    selected = categories if choice == 'A' else [categories[int(choice) - 1]]

    # 2. Build list of (url, category) tasks
    tasks = []
    for cat in selected:
        max_pages = ceil(cat["count"] / 10)
        for page in range(1, max_pages + 1):
            url = (urljoin(BASE_URL, cat["url_path"])
                   if page == 1
                   else urljoin(BASE_URL, f"{cat['url_path']}{page}/"))
            tasks.append((url, cat["name"]))

    # 3. Execute collection in parallel with progress bar
    total_pages = len(tasks)
    print(f"\nPlease wait, collecting {total_pages} pages...\n")
    all_words = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_and_parse, url, name): (url, name)
            for url, name in tasks
        }
        with tqdm(total=total_pages, desc="Collecting pages") as pbar:
            for future in as_completed(futures):
                url, name = futures[future]
                try:
                    words = future.result()
                    all_words.extend(words)
                except Exception as e:
                    print(f"[!] Error collecting {url} ({name}): {e}")
                # human‑like variable delay between 0.5 and 1 second
                time.sleep(random.uniform(0.5, 1))
                pbar.update(1)

    # 4. Write CSV
    fieldnames = [
        "dictionary_category", "grammatical_category",
        "word", "transcription", "french", "english", "arabic"
    ]
    fname = (
        "all_kabyle_words.csv"
        if len(selected) > 1
        else f"{selected[0]['name']}.csv"
    )
    with open(fname, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_words)

    print(f"\nDone! Data saved to {fname}")

if __name__ == "__main__":
    main()
