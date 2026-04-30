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
    # Modes: threadpool | asyncio | normal
    "mode": "threadpool",

    # DATE CONTROL (edit only this value)
    # "2025"                    → full year
    # "2025-07"                 → full month
    # "2025-07-03:2025-07-02"   → exact range (forward/backward both work)
    # "latest:3"                → last N days
    "date_range": "2026-04-30:2026-04-01",

    "max_db_size_mb": 50,
    "sleep": 0.3,
    "max_workers": 5,
    "retries": 3,
}
# ==========================================

BASE_URL  = "https://www.ittefaq.com.bd/api/theme_engine/get_ajax_contents"
HOME_URL  = "https://www.ittefaq.com.bd/"          # needed for cookie warm-up

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

FAILED_FILE = os.path.join(DATA_DIR, "failed_urls.txt")
PRIMARY_DB  = os.path.join(DATA_DIR, "ittefaq.db")

# ── Headers that pass Cloudflare's browser check ──────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":  "bn-BD,bn;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding":  "gzip, deflate, br",
    "Referer":          HOME_URL,
    "X-Requested-With": "XMLHttpRequest",
    "Connection":       "keep-alive",
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-origin",
}

# ─────────────────────────── SESSION (shared) ───────────────────────────────

def make_session() -> requests.Session:
    """
    Return a requests.Session pre-warmed with a homepage visit.
    The homepage sets cookies (e.g. Cloudflare __cf_bm) that the API
    endpoint requires; without them the server returns 403.
    """
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        # Visit the homepage to collect cookies
        s.get(HOME_URL, timeout=15)
        time.sleep(0.5)
    except Exception as e:
        print(f"  ⚠  Homepage warm-up failed: {e} — continuing anyway")
    return s

# One shared session for normal / threadpool modes
_SESSION: requests.Session | None = None

def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        print("  🔗 Warming up session (homepage visit) …")
        _SESSION = make_session()
    return _SESSION


# ─────────────────────────── DB ────────────────────────────

def get_db_path(index: int) -> str:
    if index == 1:
        return PRIMARY_DB
    return os.path.join(DATA_DIR, f"ittefaq_{index}.db")


def get_current_db() -> tuple[str, int]:
    i = 1
    while True:
        path = get_db_path(i)
        if not os.path.exists(path):
            return path, i
        size_mb = os.path.getsize(path) / (1024 * 1024)
        if size_mb < CONFIG["max_db_size_mb"]:
            return path, i
        i += 1


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            title          TEXT,
            url            TEXT UNIQUE,
            published_date TEXT
        )
    """)
    conn.commit()


def count_urls_in_db(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with sqlite3.connect(path) as c:
        return c.execute("SELECT COUNT(*) FROM news").fetchone()[0]


def count_all_urls() -> int:
    total, i = 0, 1
    while True:
        path = get_db_path(i)
        if not os.path.exists(path):
            break
        total += count_urls_in_db(path)
        i += 1
    return total


# ─────────────────────────── DATES ─────────────────────────

def generate_dates() -> list[str]:
    dr = CONFIG["date_range"]

    if dr.startswith("latest:"):
        days = int(dr.split(":")[1])
        return [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(days)
        ]

    if ":" in dr:
        start_str, end_str = dr.split(":")
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end   = datetime.strptime(end_str,   "%Y-%m-%d")
        step  = -1 if start >= end else 1
        dates, current = [], start
        while True:
            dates.append(current.strftime("%Y-%m-%d"))
            if current == end:
                break
            current += timedelta(days=step)
        return dates

    if len(dr) == 4:
        start = datetime(int(dr), 1, 1)
        end   = datetime(int(dr), 12, 31)
    elif len(dr) == 7:
        year, month = map(int, dr.split("-"))
        start = datetime(year, month, 1)
        end   = (datetime(year, month + 1, 1) - timedelta(days=1)
                 if month < 12 else datetime(year, 12, 31))
    else:
        raise ValueError(f"Invalid date_range: {dr!r}")

    dates, current = [], start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates[::-1]


# ─────────────────────────── PARSE ─────────────────────────

def parse_html(html: str) -> list[tuple[str, str, str]]:
    soup    = BeautifulSoup(html, "html.parser")
    results = []
    for item in soup.select("div.each"):
        a = item.select_one("a.link_overlay")
        t = item.select_one("span.time")
        if not a or not t:
            continue
        title     = a.get_text(strip=True)
        url       = a.get("href", "").strip()
        published = t.get("data-published", "").strip()
        if url.startswith("//"):
            url = "https:" + url
        results.append((title, url, published))
    return results


# ─────────────────────────── FETCH ─────────────────────────

def _params(date: str, start: int) -> dict:
    return {"widget": 565, "start": start, "count": 20, "archive_time": date}


def fetch_page(date: str, start: int) -> str:
    session = get_session()
    for attempt in range(CONFIG["retries"]):
        try:
            r = session.get(BASE_URL, params=_params(date, start), timeout=15)
            if r.status_code == 403:
                # Cloudflare challenge — back off and retry with a fresh session
                print(f"  ⚠  403 on {date} start={start} (attempt {attempt+1}) — refreshing session")
                global _SESSION
                _SESSION = make_session()
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json().get("html", "")
        except Exception as e:
            time.sleep(2 ** attempt)
    with open(FAILED_FILE, "a") as f:
        f.write(f"{date} start={start}\n")
    return ""


async def fetch_page_async(session: aiohttp.ClientSession, date: str, start: int) -> str:
    for attempt in range(CONFIG["retries"]):
        try:
            async with session.get(BASE_URL, params=_params(date, start)) as r:
                if r.status == 403:
                    await asyncio.sleep(2 ** attempt)
                    continue
                data = await r.json(content_type=None)
                return data.get("html", "")
        except Exception:
            await asyncio.sleep(2 ** attempt)
    with open(FAILED_FILE, "a") as f:
        f.write(f"{date} start={start}\n")
    return ""


# ─────────────────────────── SAVE ──────────────────────────

def save_batch(records: list[tuple], conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO news (title, url, published_date) VALUES (?, ?, ?)",
        records,
    )
    conn.commit()


# ─────────────────────────── SCRAPE ────────────────────────

def scrape_date(date: str) -> tuple[str, list]:
    all_records, start = [], 0
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


# ─────────────────────────── MODES ─────────────────────────

def run_threadpool(dates: list[str]) -> list:
    # Warm up once before spawning threads so all threads share the cookie jar
    get_session()
    results = []
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as exe:
        futures = {exe.submit(scrape_date, d): d for d in dates}
        for f in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
            results.append(f.result())
    return results


def run_normal(dates: list[str]) -> list:
    get_session()   # warm up once
    return [scrape_date(d) for d in tqdm(dates, desc="Scraping")]


async def _run_async(dates: list[str]) -> list:
    # aiohttp connector with browser headers + cookie jar
    connector = aiohttp.TCPConnector(ssl=True)
    jar       = aiohttp.CookieJar()
    async with aiohttp.ClientSession(
        headers=HEADERS,
        cookie_jar=jar,
        connector=connector,
    ) as session:
        # Homepage warm-up to collect cookies
        try:
            async with session.get(HOME_URL, timeout=aiohttp.ClientTimeout(total=15)) as r:
                await r.read()
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"  ⚠  Async homepage warm-up failed: {e}")

        results = []
        for date in tqdm(dates, desc="Scraping"):
            all_records, start = [], 0
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


def run_asyncio(dates: list[str]) -> list:
    return asyncio.run(_run_async(dates))


# ─────────────────────────── PERSIST ───────────────────────

def persist_results(results: list) -> None:
    db_path, idx = get_current_db()
    conn = sqlite3.connect(db_path)
    init_db(conn)

    last_month = None

    for date, records in results:
        if not records:
            continue

        save_batch(records, conn)

        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        if size_mb >= CONFIG["max_db_size_mb"]:
            conn.close()
            idx    += 1
            db_path = get_db_path(idx)
            conn    = sqlite3.connect(db_path)
            init_db(conn)
            print(f"  ↪ Rolled over → {os.path.basename(db_path)}")

        month = datetime.strptime(date, "%Y-%m-%d").strftime("%B %Y")
        if month != last_month:
            print(f"  ✔ Finished {month}")
            last_month = month

    conn.close()


# ─────────────────────────── REPORT ────────────────────────

def print_report() -> None:
    print("\n📊 URL count report:")
    i, total = 1, 0
    while True:
        path = get_db_path(i)
        if not os.path.exists(path):
            break
        n    = count_urls_in_db(path)
        size = os.path.getsize(path) / (1024 * 1024)
        print(f"   {os.path.basename(path):35s}  {n:>8,} URLs  {size:.1f} MB")
        total += n
        i += 1
    print(f"   {'TOTAL':35s}  {total:>8,} URLs")


# ─────────────────────────── MAIN ──────────────────────────

def main() -> None:
    dates = generate_dates()
    print(f"📅 Dates to scrape: {len(dates)}  ({dates[-1]} → {dates[0]})")

    mode = CONFIG["mode"]
    if mode == "asyncio":
        results = run_asyncio(dates)
    elif mode == "normal":
        results = run_normal(dates)
    else:
        results = run_threadpool(dates)

    persist_results(results)
    print_report()
    print("\n✅ Done — DB files saved in ./data/")


if __name__ == "__main__":
    main()
