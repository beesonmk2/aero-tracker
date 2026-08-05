"""
Aerodrome Pool Tracker — v2
---------------------------
Improvements over v1:
  * Tracks ~200+ pools per run (long-tail coverage, not just the top 80)
  * Also captures newly created pools from the "new pools" feed
  * Parses each pool's fee tier from its name and estimates 24h fee revenue
    (fees are what Predictive Allocation rewards actually track)
  * Records 1h and 24h price change (momentum context for volume spikes)
  * If an old-format history file exists, it is safely renamed to
    aerodrome_history_v1_backup.csv and a fresh v2 file is started

Still uses only free, public data (GeckoTerminal). No accounts, no API keys.
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

DEX_IDS = ["aerodrome", "aerodrome-slipstream"]
PAGES_PER_DEX = 6          # 20 pools per page -> up to ~240 pools across dexes
NEW_POOL_PAGES = 2         # pages of the network-wide "new pools" feed to scan
NETWORK = "base"
HISTORY_FILE = "aerodrome_history.csv"
BACKUP_FILE = "aerodrome_history_v1_backup.csv"
PAUSE_BETWEEN_CALLS = 1.5  # seconds; stays politely under rate limits

HEADERS = {
    "User-Agent": "aerodrome-tracker/2.0 (personal research script)",
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
    """Download a URL and return parsed JSON, or None if it fails."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"  (warning: server returned error {e.code} for {url})")
        return None
    except Exception as e:
        print(f"  (warning: could not reach {url}: {e})")
        return None


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_fee_tier(pool_name):
    """
    Pool names usually end with their fee tier, e.g. 'WETH / USDC 0.04%'.
    Returns the tier as a percentage number (0.04) or 0.0 if not found.
    """
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
        # The number Predictive Allocation actually rewards: fee revenue.
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


def collect_pools():
    pools = {}

    # 1) Main sweep: Aerodrome's pools, sorted by volume, several pages deep.
    for dex_id in DEX_IDS:
        for page in range(1, PAGES_PER_DEX + 1):
            url = (
                f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
                f"/dexes/{dex_id}/pools?page={page}&sort=h24_volume_usd_desc"
            )
            data = fetch_json(url)
            time.sleep(PAUSE_BETWEEN_CALLS)
            if not data or not data.get("data"):
                break
            for item in data["data"]:
                p = parse_pool(item, source="top_pools")
                if p["pool_address"]:
                    p["dex"] = dex_id
                    pools[p["pool_address"]] = p

    # 2) New-pools feed: catch pools at birth, before they have volume rank.
    for page in range(1, NEW_POOL_PAGES + 1):
        url = (
            f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
            f"/new_pools?page={page}&include=dex"
        )
        data = fetch_json(url)
        time.sleep(PAUSE_BETWEEN_CALLS)
        if not data or not data.get("data"):
            break
        for item in data["data"]:
            dex_id = dex_of(item)
            if "aerodrome" not in dex_id:
                continue
            p = parse_pool(item, source="new_pool")
            if p["pool_address"] and p["pool_address"] not in pools:
                p["dex"] = dex_id
                pools[p["pool_address"]] = p

    # 3) Fallback if the direct dex lookups all failed (e.g. renamed dex ids).
    if not pools:
        print("Direct lookup failed – falling back to a network-wide scan...")
        for page in range(1, 6):
            url = (
                f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
                f"/pools?page={page}&include=dex"
            )
            data = fetch_json(url)
            time.sleep(PAUSE_BETWEEN_CALLS)
            if not data or not data.get("data"):
                break
            for item in data["data"]:
                dex_id = dex_of(item)
                if "aerodrome" in dex_id:
                    p = parse_pool(item, source="fallback_scan")
                    if p["pool_address"]:
                        p["dex"] = dex_id
                        pools[p["pool_address"]] = p

    return list(pools.values())


def archive_old_format_if_needed(path, folder):
    """If an existing history file has v1 columns, rename it and start fresh."""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        if first_line and first_line.split(",") != FIELDNAMES:
            backup_path = os.path.join(folder, BACKUP_FILE)
            os.rename(path, backup_path)
            print(f"(Old-format history preserved as {BACKUP_FILE}; "
                  f"starting a fresh v2 file.)")
    except Exception:
        pass  # if anything odd happens, just append as normal


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
    by_fees = sorted(pools, key=lambda p: p["est_fees_24h_usd"], reverse=True)
    for p in by_fees[:10]:
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
        print("=" * 66)
        print(f"NEWLY CREATED AERODROME POOLS SPOTTED THIS RUN: {len(new_pools)}")
        print("=" * 66)
        for p in new_pools[:10]:
            print(f"  {p['pool_name'][:40]:<40} created {p['created_at'][:16]}")
    print()


def main():
    print("Fetching Aerodrome pool data (v2: wide sweep + new pools)...")
    print("This takes about 20-30 seconds because it politely paces itself.")
    pools = collect_pools()

    if not pools:
        print()
        print("Sorry - no data came back. This usually means either:")
        print("  1. No internet connection, or")
        print("  2. The data provider is temporarily down or rate-limiting.")
        print("Wait a minute and try again. If it keeps failing, copy this")
        print("whole message back to Claude and we'll fix it together.")
        sys.exit(1)

    snapshot_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    path = save_history(pools, snapshot_time)

    print(f"Success! Captured {len(pools)} pools at {snapshot_time} UTC.")
    print(f"Snapshot added to: {path}")
    print_summary(pools)
    print("History file grows with every run. All set.")


if __name__ == "__main__":
    main()
