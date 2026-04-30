import requests
import sqlite3
from bs4 import BeautifulSoup
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import aiohttp
from tqdm import tqdm

# ================= CONFIG =================
CONFIG = {
    "mode": "threadpool",  # threadpool | asyncio | normal

    # 🔥 DATE CONTROL (EDIT THIS ONLY)
    # --------------------------------
    # "2025"                     → full year
    # "2025-07"                 → full month
    # "2025-07-03:2025-07-02"   → exact range (forward/backward both supported)
    # "latest:3"                → last 3 days
    # --------------------------------
    "date_range": "2026-04-30:2012-11-01",

    "max_db_size_mb": 50,
    "base_db_name": "ittefaq.db",
    "sleep": 0.3,
    "max_workers": 5,
    "retries": 3
}
# ==========================================

BASE_URL = "https://www.ittefaq.com.bd/api/theme_engine/get_ajax_contents"

# 📁 DATA DIRECTORY
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

FAILED_FILE = os.path.join(DATA_DIR, "failed_urls.txt")


# ---------- DB ----------
def get_db_path(index):
    return os.path.join(DATA_DIR, f"{index}_{CONFIG['base_db_name']}")


def get_current_db():
    i = 1
    while True:
        path = get_db_path(i)

        if not os.path.exists(path):
            return path, i

        size = os.path.getsize(path) / (1024 * 1024)
        if size < CONFIG["max_db_size_mb"]:
            return path, i

        i += 1


def init_db(conn):
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        url TEXT UNIQUE,
        published_date TEXT
    )
    """)
    conn.commit()


# ---------- DATE ----------
def generate_dates():
    dr = CONFIG["date_range"]
    dates = []

    if dr.startswith("latest:"):
        days = int(dr.split(":")[1])
        for i in range(days):
            d = datetime.now() - timedelta(days=i)
            dates.append(d.strftime("%Y-%m-%d"))
        return dates

    if ":" in dr:
        start_str, end_str = dr.split(":")
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")

        step = -1 if start >= end else 1
        current = start

        while True:
            dates.append(current.strftime("%Y-%m-%d"))
            if current == end:
                break
            current += timedelta(days=step)

        return dates

    if len(dr) == 4:
        year = int(dr)
        start = datetime(year, 1, 1)
        end = datetime(year, 12, 31)

    elif len(dr) == 7:
        year, month = map(int, dr.split("-"))
        start = datetime(year, month, 1)

        if month == 12:
            end = datetime(year, 12, 31)
        else:
            end = datetime(year, month + 1, 1) - timedelta(days=1)

    else:
        raise ValueError("Invalid date_range format")

    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates[::-1]


# ---------- PARSE ----------
def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("div.each")

    results = []
    for item in items:
        a = item.select_one("a.link_overlay")
        t = item.select_one("span.time")

        if not a or not t:
            continue

        title = a.get_text(strip=True)
        url = a.get("href", "").strip()
        published = t.get("data-published", "").strip()

        if url.startswith("//"):
            url = "https:" + url

        results.append((title, url, published))

    return results


# ---------- FETCH ----------
def fetch_page(date, start):
    params = {
        "widget": 565,
        "start": start,
        "count": 250,
        "archive_time": date
    }

    for _ in range(CONFIG["retries"]):
        try:
            r = requests.get(BASE_URL, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            return data.get("html", "")
        except:
            time.sleep(1)

    with open(FAILED_FILE, "a") as f:
        f.write(f"{date} start={start}\n")
    return ""


async def fetch_page_async(session, date, start):
    params = {
        "widget": 565,
        "start": start,
        "count": 20,
        "archive_time": date
    }

    for _ in range(CONFIG["retries"]):
        try:
            async with session.get(BASE_URL, params=params) as r:
                data = await r.json()
                return data.get("html", "")
        except:
            await asyncio.sleep(1)

    with open(FAILED_FILE, "a") as f:
        f.write(f"{date} start={start}\n")
    return ""


# ---------- SAVE ----------
def save_batch(records, conn):
    c = conn.cursor()
    c.executemany(
        "INSERT OR IGNORE INTO news (title, url, published_date) VALUES (?, ?, ?)",
        records
    )
    conn.commit()


# ---------- SCRAPE ----------
def scrape_date(date):
    all_records = []
    start = 0

    while True:
        html = fetch_page(date, start)
        if not html:
            break

        rec = parse_html(html)
        if not rec:
            break

        all_records.extend(rec)
        start += 20
        time.sleep(CONFIG["sleep"])

    return date, all_records


# ---------- MODES ----------
def run_threadpool(dates):
    results = []
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as exe:
        futures = [exe.submit(scrape_date, d) for d in dates]

        for f in tqdm(as_completed(futures), total=len(futures)):
            results.append(f.result())

    return results


def run_normal(dates):
    return [scrape_date(d) for d in tqdm(dates)]


async def run_async(dates):
    results = []

    async with aiohttp.ClientSession() as session:
        for date in tqdm(dates):
            start = 0
            all_records = []

            while True:
                html = await fetch_page_async(session, date, start)
                if not html:
                    break

                rec = parse_html(html)
                if not rec:
                    break

                all_records.extend(rec)
                start += 20

            results.append((date, all_records))

    return results


# ---------- MAIN ----------
def main():
    dates = generate_dates()

    if CONFIG["mode"] == "asyncio":
        results = asyncio.run(run_async(dates))
    elif CONFIG["mode"] == "normal":
        results = run_normal(dates)
    else:
        results = run_threadpool(dates)

    db_path, idx = get_current_db()
    conn = sqlite3.connect(db_path)
    init_db(conn)

    last_month = None

    for date, records in results:
        save_batch(records, conn)

        size = os.path.getsize(db_path) / (1024 * 1024)
        if size >= CONFIG["max_db_size_mb"]:
            conn.close()
            idx += 1
            db_path = get_db_path(idx)
            conn = sqlite3.connect(db_path)
            init_db(conn)

        month = datetime.strptime(date, "%Y-%m-%d").strftime("%B %Y")
        if month != last_month:
            print(f"Finished {month}")
            last_month = month

    conn.close()
    print("✅ Done (saved in /data folder)")


if __name__ == "__main__":
    main()
