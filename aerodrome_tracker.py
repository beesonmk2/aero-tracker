"""
Aerodrome Pool Tracker — v4
---------------------------
Changes from v3:
  * Closes the "join gap": after the volume sweep, reads the gauged-pool
    list from onchain_history.csv (written by the on-chain collector) and
    fetches market data for any gauged pool the sweep missed, via the
    provider's address-based lookup. Long-tail gauged pools can no longer
    hide below the volume rankings.

Changes from v2:
  * No more guessing catalog names: the script now asks the data provider
    for its live list of exchanges on Base and tracks EVERY one whose name
    contains "aerodrome" (classic, slipstream, and anything Aero adds later)
  * Retries automatically if the provider rate-limits or hiccups
  * Prints a per-page fetch report into the run log, so a partial failure
    is loudly visible instead of silent

Same CSV format as v2 — history continues in the same file seamlessly.
"""

import csv
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ---------------------------------------------------------------- settings ---

FALLBACK_DEX_IDS = ["aerodrome", "aerodrome-slipstream"]  # used only if discovery fails
PAGES_PER_DEX = 6
NEW_POOL_PAGES = 2
ONCHAIN_FILE = "onchain_history.csv"   # written by the on-chain collector
BACKFILL_BATCH = 30        # provider allows up to 30 addresses per lookup
BACKFILL_MAX_CALLS = 20    # safety cap (30*20 = 600 backfilled pools max)
NETWORK = "base"
HISTORY_FILE = "aerodrome_history.csv"
BACKUP_FILE = "aerodrome_history_v1_backup.csv"
PAUSE_BETWEEN_CALLS = 2.0   # seconds between requests, politely slow
RETRY_WAITS = [10, 30]      # if throttled, wait 10s, then 30s, then give up

HEADERS = {
    "User-Agent": "aerodrome-tracker/3.0 (personal research script)",
    "Accept": "application/json",
}

FIELDNAMES = [
    "snapshot_utc", "source", "dex", "pool_name", "pool_address", "created_at",
    "fee_tier_pct", "tvl_usd", "volume_usd_1h", "volume_usd_6h",
    "volume_usd_24h", "est_fees_24h_usd", "trades_24h", "vol24_to_tvl",
    "price_change_1h_pct", "price_change_24h_pct",
]

# ------------------------------------------------------------- helpers -------


def fetch_json(url):
    """Download JSON with automatic retries on throttling/server errors."""
    attempts = [0] + RETRY_WAITS
    for i, wait in enumerate(attempts):
        if wait:
            print(f"    (throttled or error - waiting {wait}s and retrying...)")
            time.sleep(wait)
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {"_not_found": True}
            if e.code in (429, 500, 502, 503) and i < len(attempts) - 1:
                continue
            print(f"    (giving up on {url}: HTTP {e.code})")
            return None
        except Exception as e:
            if i < len(attempts) - 1:
                continue
            print(f"    (giving up on {url}: {e})")
            return None
    return None


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_fee_tier(pool_name):
    match = re.search(r"(\d+(?:\.\d+)?)\s*%\s*$", pool_name or "")
    return float(match.group(1)) if match else 0.0


def parse_pool(item, source):
    a = item.get("attributes", {})
    vol = a.get("volume_usd", {}) or {}
    tx = (a.get("transactions", {}) or {}).get("h24", {}) or {}
    price_chg = a.get("price_change_percentage", {}) or {}

    name = (a.get("name") or "").strip()
    tvl = to_float(a.get("reserve_in_usd"))
    vol24 = to_float(vol.get("h24"))
    fee_tier = parse_fee_tier(name)

    return {
        "source": source,
        "pool_name": name,
        "pool_address": a.get("address") or "",
        "created_at": a.get("pool_created_at") or "",
        "fee_tier_pct": fee_tier,
        "tvl_usd": round(tvl, 2),
        "volume_usd_1h": round(to_float(vol.get("h1")), 2),
        "volume_usd_6h": round(to_float(vol.get("h6")), 2),
        "volume_usd_24h": round(vol24, 2),
        "est_fees_24h_usd": round(vol24 * fee_tier / 100.0, 2),
        "trades_24h": int(to_float(tx.get("buys")) + to_float(tx.get("sells"))),
        "vol24_to_tvl": round(vol24 / tvl, 4) if tvl > 0 else 0.0,
        "price_change_1h_pct": round(to_float(price_chg.get("h1")), 3),
        "price_change_24h_pct": round(to_float(price_chg.get("h24")), 3),
    }


def dex_of(item):
    rel = item.get("relationships", {}).get("dex", {}).get("data", {}) or {}
    return rel.get("id", "")


# ------------------------------------------------------------- main ----------


def discover_aerodrome_dexes():
    """Ask the provider which exchanges exist on Base; keep anything 'aerodrome'."""
    print("Discovering Aerodrome exchange listings on Base...")
    found = []
    for page in range(1, 6):
        url = (f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
               f"/dexes?page={page}")
        data = fetch_json(url)
        time.sleep(PAUSE_BETWEEN_CALLS)
        if not data or not data.get("data"):
            break
        for item in data["data"]:
            dex_id = item.get("id", "")
            if "aerodrome" in dex_id.lower():
                found.append(dex_id)
    if found:
        print(f"  Found: {', '.join(found)}")
        return found
    print(f"  Discovery failed - falling back to known names: {FALLBACK_DEX_IDS}")
    return FALLBACK_DEX_IDS


def collect_pools():
    pools = {}
    report = []

    dex_ids = discover_aerodrome_dexes()

    for dex_id in dex_ids:
        dex_total = 0
        for page in range(1, PAGES_PER_DEX + 1):
            url = (
                f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
                f"/dexes/{dex_id}/pools?page={page}&sort=h24_volume_usd_desc"
            )
            data = fetch_json(url)
            time.sleep(PAUSE_BETWEEN_CALLS)
            if data and data.get("_not_found"):
                print(f"  {dex_id}: not found on provider (page {page}) - skipping")
                break
            if not data or not data.get("data"):
                print(f"  {dex_id} page {page}: no data returned - stopping this dex")
                break
            count = 0
            for item in data["data"]:
                p = parse_pool(item, source="top_pools")
                if p["pool_address"]:
                    p["dex"] = dex_id
                    pools[p["pool_address"].lower()] = p
                    count += 1
            dex_total += count
            print(f"  {dex_id} page {page}: {count} pools")
            if count < 20:
                break  # last page reached
        report.append(f"{dex_id}: {dex_total}")

    new_count = 0
    for page in range(1, NEW_POOL_PAGES + 1):
        url = (f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
               f"/new_pools?page={page}&include=dex")
        data = fetch_json(url)
        time.sleep(PAUSE_BETWEEN_CALLS)
        if not data or not data.get("data"):
            print(f"  new_pools page {page}: no data returned")
            break
        for item in data["data"]:
            if "aerodrome" not in dex_of(item).lower():
                continue
            p = parse_pool(item, source="new_pool")
            if p["pool_address"] and p["pool_address"] not in pools:
                p["dex"] = dex_of(item)
                pools[p["pool_address"]] = p
                new_count += 1
    report.append(f"new_pools: {new_count}")

    print()
    print("FETCH REPORT -> " + " | ".join(report))
    # 4) Join-gap backfill: fetch any gauged pool the volume sweep missed.
    gauged = load_latest_gauged_addresses()
    missing = [a for a in gauged if a not in pools]
    if gauged:
        print(f"  gauged pools known: {len(gauged)}, "
              f"missed by volume sweep: {len(missing)}")
    filled = 0
    calls = 0
    for i in range(0, len(missing), BACKFILL_BATCH):
        if calls >= BACKFILL_MAX_CALLS:
            print(f"  backfill call cap reached ({BACKFILL_MAX_CALLS}) - "
                  f"{len(missing) - i} pools left for next run")
            break
        batch = missing[i:i + BACKFILL_BATCH]
        url = (f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
               f"/pools/multi/{'%2C'.join(batch)}")
        data = fetch_json(url)
        calls += 1
        time.sleep(PAUSE_BETWEEN_CALLS)
        if not data or not data.get("data"):
            continue
        for item in data["data"]:
            p = parse_pool(item, source="gauged_backfill")
            if p["pool_address"]:
                p["dex"] = dex_of(item) or "aerodrome-gauged"
                pools[p["pool_address"].lower()] = p
                filled += 1
    if gauged:
        print(f"  backfilled market data for {filled} gauged pools "
              f"({calls} lookup calls)")

    return list(pools.values())


def load_latest_gauged_addresses():
    """Read the newest snapshot's pool addresses from the on-chain CSV."""
    folder = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(folder, ONCHAIN_FILE)
    if not os.path.exists(path):
        print("  (no onchain_history.csv yet - skipping gauged backfill)")
        return []
    try:
        latest, addrs = "", []
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ts = row.get("snapshot_utc", "")
                if ts > latest:
                    latest, addrs = ts, []
                if ts == latest and row.get("pool_address"):
                    addrs.append(row["pool_address"].lower())
        return addrs
    except Exception as e:
        print(f"  (could not read {ONCHAIN_FILE}: {e} - skipping backfill)")
        return []


def archive_old_format_if_needed(path, folder):
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        if first_line and first_line.split(",") != FIELDNAMES:
            os.rename(path, os.path.join(folder, BACKUP_FILE))
            print(f"(Old-format history preserved as {BACKUP_FILE}.)")
    except Exception:
        pass


def save_history(pools, snapshot_time):
    folder = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(folder, HISTORY_FILE)
    archive_old_format_if_needed(path, folder)

    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for p in pools:
            writer.writerow({"snapshot_utc": snapshot_time, **p})
    return path


def print_summary(pools):
    print()
    print("=" * 66)
    print("TOP 10 POOLS BY ESTIMATED 24H FEE REVENUE (what rewards track)")
    print("=" * 66)
    for p in sorted(pools, key=lambda p: p["est_fees_24h_usd"], reverse=True)[:10]:
        print(f"  {p['pool_name'][:32]:<32} "
              f"${p['est_fees_24h_usd']:>10,.0f} fees   "
              f"${p['volume_usd_24h']:>12,.0f} vol")

    print()
    print("=" * 66)
    print("TOP 10 BY VOLUME/TVL RATIO (busiest liquidity, min $100k TVL)")
    print("=" * 66)
    busy = [p for p in pools if p["tvl_usd"] >= 100_000]
    for p in sorted(busy, key=lambda p: p["vol24_to_tvl"], reverse=True)[:10]:
        print(f"  {p['pool_name'][:32]:<32} "
              f"ratio {p['vol24_to_tvl']:>7.2f}   "
              f"${p['volume_usd_24h']:>12,.0f} vol")

    new_pools = [p for p in pools if p["source"] == "new_pool"]
    if new_pools:
        print()
        print(f"NEWLY CREATED AERODROME POOLS THIS RUN: {len(new_pools)}")
        for p in new_pools[:10]:
            print(f"  {p['pool_name'][:40]:<40} created {p['created_at'][:16]}")
    print()


def main():
    print("Aerodrome tracker v3 starting...")
    pools = collect_pools()

    if not pools:
        print()
        print("Sorry - no data came back at all. Likely a provider outage or")
        print("no internet. Try again in a few minutes; if it keeps failing,")
        print("copy this whole log back to Claude.")
        sys.exit(1)

    snapshot_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    path = save_history(pools, snapshot_time)

    print(f"Success! Captured {len(pools)} pools at {snapshot_time} UTC.")
    print(f"Snapshot added to: {path}")
    print_summary(pools)


if __name__ == "__main__":
    main()
