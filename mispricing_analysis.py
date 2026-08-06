"""
Aerodrome Mispricing Analysis — Stage 1 (v1.1)
----------------------------------------------
v1.1: prices EVERY reward token by on-chain address (batched lookups),
ranks the headline table by excess dollars instead of raw ratio, and
summarizes unpriced leftovers instead of listing hundreds of tokens.

The research instrument. On each run it:
  1. Loads the LATEST snapshot from onchain_history.csv (votes, emissions,
     epoch fee rewards) and aerodrome_history.csv (volume, TVL, names)
  2. Prices the epoch fee rewards in USD (live token prices for the major
     reward tokens; anything unpriceable is reported separately, never
     silently dropped)
  3. Computes each pool's share of total fees vs share of total votes.
     ratio > 1  -> UNDER-allocated (earning more than voters reward it)
     ratio < 1  -> OVER-allocated  (rewarded beyond what it earns)
  4. Measures vote velocity vs the previous on-chain snapshot
  5. Writes analysis/mispricing_latest.md (+ .csv) and a timestamped copy
     in analysis/history/

Outputs are research-grade: early in an epoch fee totals are small and
rankings noisy. Trust trends over single runs.
"""

import csv
import json
import os
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

# ---------------------------------------------------------------- settings ---

MARKET_FILE = "aerodrome_history.csv"
ONCHAIN_FILE = "onchain_history.csv"
OUT_DIR = "analysis"

MIN_FEES_USD_FOR_RANKING = 100.0   # ignore dust when ranking under-allocated
MIN_VOTE_SHARE_FOR_OVER = 0.001    # 0.1%+ of votes to count as over-allocated
TOP_N = 15

# Known Base token addresses for the reward tokens that carry most fee value.
# Symbols not listed here are reported in the "unpriced" bucket.
TOKEN_ADDRESSES = {
    "WETH":  "0x4200000000000000000000000000000000000006",
    "USDC":  "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
    "AERO":  "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
    "cbBTC": "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf",
    "cbETH": "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",
    "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
    "weETH": "0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A",
    "DAI":   "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
    "EURC":  "0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42",
    "tBTC":  "0x236aa50979D5f3De3Bd1Eeb40E81137F22ab794b",
    "VIRTUAL": "0x0b3e328455c4059EEb9e3f84b5543F74E24e7E1b",
}
# If the live price lookup fails entirely, these keep stables usable.
STABLE_FALLBACK = {"USDC": 1.0, "USDbC": 1.0, "DAI": 1.0}

HEADERS = {"User-Agent": "aero-mispricing/1.0", "Accept": "application/json"}


# ------------------------------------------------------------- data loading --


def load_latest_snapshots(path, n=2):
    """Return the last n snapshots from a history CSV as {ts: [rows]}."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} not found - has its collector run at least once?")
    by_ts = defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_ts[row["snapshot_utc"]].append(row)
    ordered = sorted(by_ts.keys())
    return {ts: by_ts[ts] for ts in ordered[-n:]}


def parse_reward_key(key):
    """'SYM@0xaddr' -> (sym, addr); legacy bare 'SYM' -> (sym, known addr or None)."""
    if "@" in key:
        sym, addr = key.rsplit("@", 1)
        return sym, addr.lower()
    return key, TOKEN_ADDRESSES.get(key, "").lower() or None


def collect_reward_addresses(snapshot_rows):
    addrs = set()
    for r in snapshot_rows:
        for col in ("fees_rewards", "bribe_rewards"):
            for key in json.loads(r.get(col) or "{}"):
                _, addr = parse_reward_key(key)
                if addr:
                    addrs.add(addr)
    return sorted(addrs)


def fetch_prices_by_address(addresses):
    """Batched GeckoTerminal price lookups: every address, 30 per call."""
    prices = {}
    failed_batches = 0
    for i in range(0, len(addresses), 30):
        batch = addresses[i:i + 30]
        url = ("https://api.geckoterminal.com/api/v2/simple/networks/base/"
               "token_price/" + "%2C".join(batch))
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            raw = data["data"]["attributes"]["token_prices"] or {}
            for k, v in raw.items():
                if v is not None:
                    prices[k.lower()] = float(v)
        except Exception as e:
            failed_batches += 1
            print(f"  (price batch {i//30 + 1} failed: {e})")
        time.sleep(1.0)
    print(f"Prices resolved for {len(prices)}/{len(addresses)} reward tokens"
          + (f" ({failed_batches} batch(es) failed)" if failed_batches else ""))
    # stable fallback for legacy rows if everything failed
    if not prices:
        print("WARNING: no live prices at all - stablecoin fallback only.")
        for sym, addr in TOKEN_ADDRESSES.items():
            if sym in STABLE_FALLBACK:
                prices[addr.lower()] = 1.0
    return prices


def price_rewards(rewards_json, prices):
    """JSON {'SYM@addr': amt} (or legacy {'SYM': amt}) -> (usd, unpriced)."""
    usd, unpriced = 0.0, {}
    for key, amt in json.loads(rewards_json or "{}").items():
        sym, addr = parse_reward_key(key)
        if addr and addr in prices:
            usd += amt * prices[addr]
        else:
            unpriced[sym] = unpriced.get(sym, 0) + amt
    return usd, unpriced


# ------------------------------------------------------------- analysis ------


def build_table():
    onchain = load_latest_snapshots(ONCHAIN_FILE, n=2)
    market = load_latest_snapshots(MARKET_FILE, n=1)

    ts_list = list(onchain.keys())
    latest_ts = ts_list[-1]
    prev_ts = ts_list[-2] if len(ts_list) > 1 else None
    latest = onchain[latest_ts]
    prev_votes = {}
    if prev_ts:
        prev_votes = {r["pool_address"]: float(r["votes_veaero"])
                      for r in onchain[prev_ts]}

    market_ts = list(market.keys())[-1]
    mkt = {r["pool_address"].lower(): r for r in market[market_ts]}

    reward_addrs = collect_reward_addresses(latest)
    print(f"Distinct reward tokens in snapshot: {len(reward_addrs)}")
    prices = fetch_prices_by_address(reward_addrs)

    pools = []
    unpriced_totals = defaultdict(float)
    for r in latest:
        addr = r["pool_address"].lower()
        fees_usd, unpriced = price_rewards(r["fees_rewards"], prices)
        bribes_usd, unpriced_b = price_rewards(r["bribe_rewards"], prices)
        for s, a in {**unpriced, **unpriced_b}.items():
            unpriced_totals[s] += a
        m = mkt.get(addr, {})
        votes = float(r["votes_veaero"])
        pools.append({
            "pool_address": addr,
            "pool_name": m.get("pool_name", "") or addr[:10] + "...",
            "votes": votes,
            "votes_prev": prev_votes.get(r["pool_address"]),
            "emissions_day": float(r["emissions_aero_per_day"]),
            "fees_usd": round(fees_usd, 2),
            "bribes_usd": round(bribes_usd, 2),
            "unpriced": json.dumps(unpriced) if unpriced else "",
            "vol24_usd": float(m.get("volume_usd_24h", 0) or 0),
            "tvl_usd": float(m.get("tvl_usd", 0) or 0),
            "est_trading_fees_24h": float(m.get("est_fees_24h_usd", 0) or 0),
            "epoch_start": r["epoch_start_utc"],
        })

    total_votes = sum(p["votes"] for p in pools) or 1.0
    total_fees = sum(p["fees_usd"] for p in pools) or 1.0
    for p in pools:
        p["vote_share"] = p["votes"] / total_votes
        p["fee_share"] = p["fees_usd"] / total_fees
        p["ratio"] = (p["fee_share"] / p["vote_share"]
                      if p["vote_share"] > 0 else None)
        # dollars of epoch fees beyond (or below) the pool's fair share
        p["excess_usd"] = round(p["fees_usd"] - p["vote_share"] * total_fees, 2)
        if p["votes_prev"] is not None:
            p["vote_delta"] = p["votes"] - p["votes_prev"]
            p["vote_delta_pct"] = (100 * p["vote_delta"] / p["votes_prev"]
                                   if p["votes_prev"] > 0 else None)
        else:
            p["vote_delta"] = None
            p["vote_delta_pct"] = None

    meta = {
        "onchain_ts": latest_ts, "prev_ts": prev_ts, "market_ts": market_ts,
        "total_votes": total_votes, "total_fees_usd": total_fees,
        "n_pools": len(pools),
        "epoch_start": pools[0]["epoch_start"] if pools else "?",
        "unpriced_totals": dict(unpriced_totals),
        "n_priced_tokens": len(prices),
    }
    return pools, meta


# ------------------------------------------------------------- reporting -----


def fmt_usd(x):
    return f"${x:,.0f}"


def md_row(cells):
    return "| " + " | ".join(str(c) for c in cells) + " |"


def write_reports(pools, meta):
    os.makedirs(os.path.join(OUT_DIR, "history"), exist_ok=True)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%d_%H%M")

    epoch_start = meta["epoch_start"]
    try:
        es = datetime.strptime(epoch_start, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
        hours_in = (now - es).total_seconds() / 3600
        epoch_note = f"{epoch_start} UTC ({hours_in:.1f}h into epoch)"
        early = hours_in < 24
    except Exception:
        epoch_note, early = epoch_start, False

    under_excess = sorted([p for p in pools if p["excess_usd"] > 0],
                          key=lambda p: p["excess_usd"], reverse=True)[:TOP_N]
    under = sorted([p for p in pools if p["ratio"] is not None
                    and p["fees_usd"] >= MIN_FEES_USD_FOR_RANKING],
                   key=lambda p: p["ratio"], reverse=True)[:TOP_N]
    over = sorted([p for p in pools if p["ratio"] is not None
                   and p["vote_share"] >= MIN_VOTE_SHARE_FOR_OVER],
                  key=lambda p: p["ratio"])[:TOP_N]
    orphans = sorted([p for p in pools if p["votes"] == 0
                      and p["fees_usd"] > 0],
                     key=lambda p: p["fees_usd"], reverse=True)[:10]
    movers = [p for p in pools if p["vote_delta"] is not None]
    gainers = sorted(movers, key=lambda p: p["vote_delta"], reverse=True)[:10]
    losers = sorted(movers, key=lambda p: p["vote_delta"])[:10]

    L = []
    L.append("# Aerodrome Fees-vs-Votes Mispricing Report")
    L.append("")
    L.append(f"*Generated {now.strftime('%Y-%m-%d %H:%M')} UTC · "
             f"on-chain snapshot {meta['onchain_ts']} · "
             f"market snapshot {meta['market_ts']}*")
    L.append("")
    L.append(f"- Epoch start: **{epoch_note}**")
    L.append(f"- Gauged pools analyzed: **{meta['n_pools']}**")
    L.append(f"- Total veAERO votes: **{meta['total_votes']:,.0f}**")
    L.append(f"- Total epoch fee rewards priced: "
             f"**{fmt_usd(meta['total_fees_usd'])}**")
    if early:
        L.append("")
        L.append("> **Early-epoch caution:** fees have only accumulated for a "
                 "few hours, so ratios are noisy. Trust trends across runs, "
                 "not single readings.")
    if meta["unpriced_totals"]:
        n_un = len(meta["unpriced_totals"])
        sample = ", ".join(sorted(meta["unpriced_totals"])[:8])
        L.append("")
        L.append(f"> {n_un} reward token(s) could not be priced this run "
                 f"(e.g. {sample}). Their value is excluded from USD totals; "
                 f"full amounts are in the CSV's 'unpriced' column.")
    L.append("")

    L.append("## Biggest mispricings by dollars")
    L.append("")
    L.append("*Excess $ = epoch fees beyond what the pool's vote share "
             "'deserves'. This is where the most absolute money is being "
             "left on the table.*")
    L.append("")
    L.append(md_row(["Pool", "Excess $", "Ratio", "Fees (epoch)", "Vote %",
                     "24h Vol"]))
    L.append(md_row(["---"] * 6))
    for p in under_excess:
        L.append(md_row([p["pool_name"], fmt_usd(p["excess_usd"]),
                         f"{p['ratio']:.2f}" if p["ratio"] else "-",
                         fmt_usd(p["fees_usd"]),
                         f"{100*p['vote_share']:.3f}%",
                         fmt_usd(p["vol24_usd"])]))
    L.append("")

    L.append("## Highest ratios (small-pool opportunities, min "
             f"${MIN_FEES_USD_FOR_RANKING:.0f} fees)")
    L.append("")
    L.append("*ratio = fee share / vote share. Tiny vote shares inflate "
             "ratios, so treat this as a candidate list, not a ranking.*")
    L.append("")
    L.append(md_row(["Pool", "Ratio", "Fees (epoch)", "Vote %", "24h Vol",
                     "Vote Δ"]))
    L.append(md_row(["---"] * 6))
    for p in under:
        delta = (f"{p['vote_delta']:+,.0f}" if p["vote_delta"] is not None
                 else "n/a")
        L.append(md_row([p["pool_name"], f"{p['ratio']:.2f}",
                         fmt_usd(p["fees_usd"]),
                         f"{100*p['vote_share']:.3f}%",
                         fmt_usd(p["vol24_usd"]), delta]))
    L.append("")

    L.append("## Over-allocated - vote share exceeds fee share")
    L.append("")
    L.append(md_row(["Pool", "Ratio", "Fees (epoch)", "Vote %",
                     "Emissions/day", "Vote Δ"]))
    L.append(md_row(["---"] * 6))
    for p in over:
        delta = (f"{p['vote_delta']:+,.0f}" if p["vote_delta"] is not None
                 else "n/a")
        L.append(md_row([p["pool_name"], f"{p['ratio']:.2f}",
                         fmt_usd(p["fees_usd"]),
                         f"{100*p['vote_share']:.3f}%",
                         f"{p['emissions_day']:,.0f} AERO", delta]))
    L.append("")

    if orphans:
        L.append("## Zero votes, nonzero fees - money on the table")
        L.append("")
        L.append(md_row(["Pool", "Fees (epoch)", "24h Vol", "TVL"]))
        L.append(md_row(["---"] * 4))
        for p in orphans:
            L.append(md_row([p["pool_name"], fmt_usd(p["fees_usd"]),
                             fmt_usd(p["vol24_usd"]), fmt_usd(p["tvl_usd"])]))
        L.append("")

    L.append("## Vote velocity since previous snapshot"
             + (f" ({meta['prev_ts']})" if meta['prev_ts'] else ""))
    L.append("")
    if not movers:
        L.append("*Only one on-chain snapshot available - velocity appears "
                 "from the second snapshot onward.*")
    else:
        L.append("**Gaining votes fastest:**")
        L.append("")
        L.append(md_row(["Pool", "Vote Δ", "Δ %", "Ratio", "Fees (epoch)"]))
        L.append(md_row(["---"] * 5))
        for p in gainers:
            pct = (f"{p['vote_delta_pct']:+.1f}%"
                   if p["vote_delta_pct"] is not None else "new")
            L.append(md_row([p["pool_name"], f"{p['vote_delta']:+,.0f}", pct,
                             f"{p['ratio']:.2f}" if p["ratio"] else "-",
                             fmt_usd(p["fees_usd"])]))
        L.append("")
        L.append("**Losing votes fastest:**")
        L.append("")
        L.append(md_row(["Pool", "Vote Δ", "Δ %", "Ratio", "Fees (epoch)"]))
        L.append(md_row(["---"] * 5))
        for p in losers:
            pct = (f"{p['vote_delta_pct']:+.1f}%"
                   if p["vote_delta_pct"] is not None else "-")
            L.append(md_row([p["pool_name"], f"{p['vote_delta']:+,.0f}", pct,
                             f"{p['ratio']:.2f}" if p["ratio"] else "-",
                             fmt_usd(p["fees_usd"])]))
    L.append("")
    L.append("---")
    L.append("*Research instrument, not investment advice. Ratios compare "
             "voter fee rewards to votes within the current epoch only.*")

    report = "\n".join(L)
    latest_md = os.path.join(OUT_DIR, "mispricing_latest.md")
    with open(latest_md, "w", encoding="utf-8") as f:
        f.write(report)
    with open(os.path.join(OUT_DIR, "history", f"mispricing_{stamp}.md"),
              "w", encoding="utf-8") as f:
        f.write(report)

    fields = ["pool_address", "pool_name", "votes", "vote_share", "fees_usd",
              "fee_share", "ratio", "excess_usd", "bribes_usd", "emissions_day",
              "vol24_usd", "tvl_usd", "est_trading_fees_24h", "vote_delta",
              "vote_delta_pct", "unpriced"]
    csv_path = os.path.join(OUT_DIR, "mispricing_latest.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for p in sorted(pools, key=lambda p: p["fees_usd"], reverse=True):
            w.writerow(p)

    return latest_md, csv_path


def main():
    print("Mispricing analysis starting...")
    pools, meta = build_table()
    md, csvp = write_reports(pools, meta)
    print(f"Analyzed {meta['n_pools']} gauged pools.")
    print(f"Total epoch fees priced: ${meta['total_fees_usd']:,.0f} "
          f"across {meta['total_votes']:,.0f} votes.")
    print(f"Report written: {md}")
    print(f"Data written:   {csvp}")
    print("Open the .md file in your repository - GitHub renders it as a "
          "formatted page.")


if __name__ == "__main__":
    main()
