"""
volume_backfill.py  (v1.2)

v1.2: The free API's 180-day history limit (HTTP 401 "upgrade to access
      data beyond 180 days") is now recognized as the natural edge of
      available history: pagination stops gracefully and keeps everything
      gathered, with no retry storm. 180 days x every pool is the maximum
      the free tier allows; the hourly tracker extends the record forward
      from here.

v1.1: - Pagination fix: keep requesting older pages until the API returns
        an EMPTY page, instead of stopping when a page has fewer candles
        than requested (the API serves ~180 candles per response no matter
        what limit is asked for). This tests whether deep history exists
        beyond the ~6-month window every pool returned in v1.0.
      - Schema marker added: shallow v1.0 files are wiped and refetched.
      - Politeness spacing raised to reduce rate-limit pauses.

Fetches full daily price/volume history (OHLCV candles) for every pool that
has ever appeared in Aerodrome's vote history, using GeckoTerminal's free
API, and saves one CSV per pool under data/history/volume/.

How it works, in plain English:
  - The pool universe comes from data/history/votes/*.csv — the files the
    vote backfill produces. Every pool that ever had votes is exactly the
    set of pools that matters for backtesting. No hardcoded DEX IDs.
  - For each pool we request daily candles (date, open, high, low, close,
    volume in USD) going back to Aerodrome's launch in August 2023.
    GeckoTerminal serves up to 1,000 days per request, so a pool's whole
    history usually takes 2 requests.
  - GeckoTerminal's free tier allows ~30 calls per minute; the script paces
    itself to stay under that.
  - A pool's output file existing = that pool is done. Re-running the
    script only fetches what's missing, so it can be run again after the
    vote backfill finishes to pick up newly discovered pools.

Modes (set via the MODE environment variable):
  MODE=sample  -> fetch only the first 15 pools, to validate the format.
  MODE=full    -> fetch every pool not yet done.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API = "https://api.geckoterminal.com/api/v2"
NETWORK = "base"

VOTES_DIR = os.path.join("data", "history", "votes")
OUT_DIR = os.path.join("data", "history", "volume")
INDEX_FILE = os.path.join(OUT_DIR, "_pools_index.csv")

# Don't bother fetching candles from before Aerodrome existed.
EARLIEST_TS = int(datetime(2023, 8, 1, tzinfo=timezone.utc).timestamp())

# GeckoTerminal free tier: ~30 calls/minute. 2.2s spacing keeps us safely
# under it.
CALL_SPACING_SECONDS = 2.7

# Stop cleanly after this many minutes so GitHub can commit the results.
MAX_MINUTES = int(os.environ.get("MAX_MINUTES", "300"))

FIELDNAMES = ["date", "open", "high", "low", "close", "volume_usd"]

SCHEMA_VERSION = 2
SCHEMA_MARKER = os.path.join(OUT_DIR, "_schema.json")


def ensure_schema():
    """Wipe files written by an older version of this script (they may be
    shallow) and stamp the folder with the current schema version."""
    os.makedirs(OUT_DIR, exist_ok=True)
    current = 0
    if os.path.exists(SCHEMA_MARKER):
        try:
            with open(SCHEMA_MARKER) as f:
                current = json.load(f).get("version", 0)
        except Exception:  # noqa: BLE001
            current = 0
    if current != SCHEMA_VERSION:
        removed = 0
        for name in os.listdir(OUT_DIR):
            p = os.path.join(OUT_DIR, name)
            if os.path.isfile(p):
                os.remove(p)
                removed += 1
        with open(SCHEMA_MARKER, "w") as f:
            json.dump({"version": SCHEMA_VERSION}, f)
        print(f"[schema] Cleared {removed} files from an older script "
              f"version; they will be refetched at full depth.", flush=True)


# ---------------------------------------------------------------------------
# Pool universe from the vote history
# ---------------------------------------------------------------------------

def discover_pools():
    """Collect every distinct pool address that appears in the vote history
    files, most-voted pools first (so the most important data lands
    earliest if a run gets cut short)."""
    totals = {}
    if not os.path.isdir(VOTES_DIR):
        return []
    for name in sorted(os.listdir(VOTES_DIR)):
        if not name.endswith(".csv"):
            continue
        with open(os.path.join(VOTES_DIR, name), newline="") as f:
            for r in csv.DictReader(f):
                pool = r["pool"].lower()
                try:
                    w = float(r.get("final_weight") or 0)
                except ValueError:
                    w = 0.0
                totals[pool] = totals.get(pool, 0.0) + w
    return [p for p, _ in sorted(totals.items(), key=lambda kv: -kv[1])]


# ---------------------------------------------------------------------------
# GeckoTerminal fetching
# ---------------------------------------------------------------------------

_last_call = 0.0


def api_get(path, params):
    """One paced GET request with retry on rate limiting and flakiness."""
    global _last_call
    for attempt in range(6):
        wait = CALL_SPACING_SECONDS - (time.time() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.time()
        try:
            r = requests.get(API + path, params=params,
                             headers={"accept": "application/json"},
                             timeout=30)
        except Exception as e:  # noqa: BLE001
            print(f"[api-err] network hiccup: {type(e).__name__}: "
                  f"{str(e)[:150]} — retrying", flush=True)
            time.sleep(5)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None  # pool not indexed by GeckoTerminal
        if r.status_code == 401 and "180 days" in r.text:
            # The free API's history limit — not an error, just the edge of
            # what we're allowed to see. Keep everything gathered so far.
            return "HISTORY_LIMIT"
        if r.status_code == 429:
            print("[api-err] rate limited — pausing 65s", flush=True)
            time.sleep(65)
            continue
        print(f"[api-err] HTTP {r.status_code}: {r.text[:150]} — retrying",
              flush=True)
        time.sleep(10)
    raise RuntimeError(f"GeckoTerminal kept failing for {path}")


def fetch_pool_history(pool):
    """Fetch all daily candles for one pool, oldest to newest.
    Returns (rows, meta) where rows is a list of candle dicts and meta is
    whatever token info GeckoTerminal includes (may be None).
    Returns (None, None) if the pool isn't indexed at all."""
    rows = {}
    meta = None
    before = int(time.time())
    prev_oldest = None
    for _page in range(14):  # 14 pages x ~180 days comfortably covers 3+ years
        data = api_get(
            f"/networks/{NETWORK}/pools/{pool}/ohlcv/day",
            {"aggregate": 1, "limit": 1000, "currency": "usd",
             "before_timestamp": before},
        )
        if data == "HISTORY_LIMIT":
            break  # free-API history edge reached; keep what we have
        if data is None:
            return (None, None) if not rows else (list(rows.values()), meta)
        if meta is None:
            meta = data.get("meta")
        candles = (data.get("data", {}) or {}).get(
            "attributes", {}).get("ohlcv_list") or []
        if not candles:
            break  # a truly EMPTY page means we've reached the beginning
        for c in candles:
            ts = int(c[0])
            rows[ts] = {
                "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                "volume_usd": c[5],
            }
        oldest = min(int(c[0]) for c in candles)
        if oldest <= EARLIEST_TS:
            break  # reached back before Aerodrome existed
        if prev_oldest is not None and oldest >= prev_oldest:
            break  # no progress: API won't serve anything older
        prev_oldest = oldest
        before = oldest
    ordered = [rows[ts] for ts in sorted(rows)]
    return ordered, meta


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def pool_path(pool):
    return os.path.join(OUT_DIR, f"{pool}.csv")


def write_pool(pool, rows):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(pool_path(pool), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def append_index(pool, meta, n_days):
    """Keep a human-readable directory of which pool is which."""
    os.makedirs(OUT_DIR, exist_ok=True)
    base = quote = ""
    try:
        base = meta["base"]["symbol"]
        quote = meta["quote"]["symbol"]
    except Exception:  # noqa: BLE001
        pass
    exists = os.path.exists(INDEX_FILE)
    with open(INDEX_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["pool", "base_symbol", "quote_symbol", "days"])
        w.writerow([pool, base, quote, n_days])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    mode = os.environ.get("MODE", "sample").lower()
    start = time.time()

    ensure_schema()

    pools = discover_pools()
    if not pools:
        print("[main] No vote history files found yet in data/history/votes/. "
              "Run the History Backfill workflow first — it builds the pool "
              "universe this script feeds on.", flush=True)
        return 1

    todo = [p for p in pools if not os.path.exists(pool_path(p))]
    print(f"[main] mode={mode}: {len(pools)} pools known from vote history, "
          f"{len(pools) - len(todo)} already fetched, {len(todo)} to go.",
          flush=True)

    if mode == "sample":
        todo = todo[:15]

    done = skipped = 0
    for i, pool in enumerate(todo, 1):
        elapsed_min = (time.time() - start) / 60
        if elapsed_min >= MAX_MINUTES:
            print(f"[main] time budget ({MAX_MINUTES} min) reached — "
                  f"stopping cleanly. Run again to continue.", flush=True)
            break

        rows, meta = fetch_pool_history(pool)
        if rows is None:
            # Not indexed by GeckoTerminal: write an empty file so we don't
            # ask again every run.
            write_pool(pool, [])
            append_index(pool, None, 0)
            skipped += 1
            print(f"[{i}/{len(todo)}] {pool} — not indexed, marked skipped",
                  flush=True)
            continue

        write_pool(pool, rows)
        append_index(pool, meta, len(rows))
        done += 1
        label = ""
        try:
            label = f" ({meta['base']['symbol']}/{meta['quote']['symbol']})"
        except Exception:  # noqa: BLE001
            pass
        print(f"[{i}/{len(todo)}] {pool}{label} — {len(rows)} days saved",
              flush=True)

    remaining = len(todo) - done - skipped
    print(f"[main] Saved {done} pools, {skipped} not indexed, "
          f"{remaining} remaining.", flush=True)
    if remaining == 0 and mode == "full":
        print("[main] 🎉 VOLUME BACKFILL COMPLETE for all currently known "
              "pools. Re-run after the vote backfill finishes to catch any "
              "newly discovered pools.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
