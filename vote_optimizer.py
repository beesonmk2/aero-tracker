"""
veAERO Vote Plan Generator — v1
-------------------------------
Decision-support for weekly gauge voting. On each run it:
  1. Reads the freshly generated analysis/mispricing_latest.csv
     (fees, bribes, votes per pool - already USD-priced)
  2. Projects each pool's FULL-epoch rewards:
       fees scaled linearly to 168h; bribes taken as-is (lump deposits)
  3. Water-fills YOUR votes across pools to maximize projected rewards,
     accounting for how your own votes dilute each pool's per-vote yield
  4. Writes voteplan/vote_plan_latest.md (+ timestamped history copy)

This is a computation of projected yields under stated assumptions,
not financial advice. Assumptions are printed with every plan.
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

# ------------------------------------------------------------- your config ---

# NFT 3 (159,599.84) is deposited with 40 Acres as the benchmark cohort -
# this optimizer plans only the self-voted NFTs below.
MY_VENFTS = {
    "NFT 1": 32966.88,
    "NFT 2": 153440.57,
}
CHUNK = 500.0            # allocation granularity in veAERO
MAX_POOLS = 8            # cap the plan at this many pools (gas + sanity)
MIN_POOL_FEES_USD = 50.0 # ignore pools with essentially no epoch revenue yet
EPOCH_HOURS = 168.0

ANALYSIS_CSV = os.path.join("analysis", "mispricing_latest.csv")
ONCHAIN_FILE = "onchain_history.csv"
OUT_DIR = "voteplan"


# ------------------------------------------------------------- loading -------


def load_pools():
    if not os.path.exists(ANALYSIS_CSV):
        raise FileNotFoundError(
            f"{ANALYSIS_CSV} missing - the workflow runs the analysis first, "
            f"so this usually means that step failed.")
    pools = []
    with open(ANALYSIS_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            pools.append({
                "name": r["pool_name"],
                "address": r["pool_address"],
                "votes": float(r["votes"] or 0),
                "fees_usd": float(r["fees_usd"] or 0),
                "bribes_usd": float(r["bribes_usd"] or 0),
                "unpriced": r.get("unpriced", ""),
            })
    return pools


def hours_into_epoch():
    """Read epoch start from the newest on-chain row; return hours elapsed."""
    last = None
    with open(ONCHAIN_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            last = row
    es = datetime.strptime(last["epoch_start_utc"],
                           "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    hours = (datetime.now(timezone.utc) - es).total_seconds() / 3600
    return max(hours, 0.5), es


# ------------------------------------------------------------- optimizer -----


def project_rewards(pools, hours):
    """Full-epoch projected rewards per pool under stated assumptions."""
    scale = EPOCH_HOURS / hours
    for p in pools:
        p["proj_rewards_usd"] = p["fees_usd"] * scale + p["bribes_usd"]
    return pools


def water_fill(pools, my_total):
    """Greedy chunk allocation maximizing sum of my share of pool rewards.

    My reward from pool i if I add v_i votes:
        proj_rewards_i * v_i / (votes_i + v_i)
    Each chunk goes to the pool with the highest marginal gain.
    """
    cands = [p for p in pools
             if p["proj_rewards_usd"] > 0 and p["fees_usd"] >= MIN_POOL_FEES_USD]
    alloc = defaultdict(float)

    def my_reward(p, mine):
        return p["proj_rewards_usd"] * mine / (p["votes"] + mine) if mine else 0.0

    remaining = my_total
    while remaining > 1e-9:
        step = min(CHUNK, remaining)
        best, best_gain = None, 0.0
        for p in cands:
            mine = alloc[p["address"]]
            if mine == 0 and len([a for a in alloc.values() if a > 0]) >= MAX_POOLS:
                continue  # respect the pool-count cap for new entries
            gain = (my_reward(p, mine + step) - my_reward(p, mine))
            if gain > best_gain:
                best, best_gain = p, gain
        if best is None:
            break
        alloc[best["address"]] += step
        remaining -= step

    plan = []
    for p in cands:
        mine = alloc[p["address"]]
        if mine > 0:
            plan.append({
                **p,
                "my_votes": mine,
                "my_share_pct": 100 * mine / (p["votes"] + mine),
                "my_proj_usd": my_reward(p, mine),
                "pct_of_my_votes": 100 * mine / my_total,
            })
    plan.sort(key=lambda x: x["my_proj_usd"], reverse=True)
    return plan


def baseline_single_pool(pools, my_total):
    """Best achievable by putting everything on one pool (for comparison)."""
    best = 0.0, None
    for p in pools:
        if p["proj_rewards_usd"] <= 0 or p["fees_usd"] < MIN_POOL_FEES_USD:
            continue
        r = p["proj_rewards_usd"] * my_total / (p["votes"] + my_total)
        if r > best[0]:
            best = r, p
    return best


# ------------------------------------------------------------- report --------


def fmt(x):
    return f"${x:,.2f}"


def write_plan(plan, meta):
    os.makedirs(os.path.join(OUT_DIR, "history"), exist_ok=True)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%d_%H%M")
    my_total = meta["my_total"]
    plan_total = sum(p["my_proj_usd"] for p in plan)

    L = []
    L.append("# veAERO Vote Plan")
    L.append("")
    L.append(f"*Generated {now.strftime('%Y-%m-%d %H:%M')} UTC · "
             f"{meta['hours']:.1f}h into epoch (started {meta['epoch_start']})*")
    L.append("")
    L.append(f"- Your voting power: **{my_total:,.2f} veAERO** across "
             f"{len(MY_VENFTS)} NFTs")
    L.append(f"- Projected epoch rewards under this plan: "
             f"**{fmt(plan_total)}**")
    single_r, single_p = meta["baseline"]
    if single_p:
        gain = plan_total - single_r
        L.append(f"- Best single-pool alternative ({single_p['name']}): "
                 f"{fmt(single_r)} - the split plan projects "
                 f"**{fmt(gain)} more** ({100*gain/single_r:.1f}% better)"
                 if single_r > 0 else "")
    L.append("")
    L.append("## Recommended split")
    L.append("")
    L.append("*Apply the same percentages to each of your three NFTs when "
             "voting - votes are fungible across NFTs for this purpose.*")
    L.append("")
    L.append("| Pool | % of your votes | veAERO | Your share of pool | "
             "Projected reward |")
    L.append("| --- | --- | --- | --- | --- |")
    for p in plan:
        L.append(f"| {p['name']} | {p['pct_of_my_votes']:.1f}% | "
                 f"{p['my_votes']:,.0f} | {p['my_share_pct']:.2f}% | "
                 f"{fmt(p['my_proj_usd'])} |")
    L.append("")
    L.append("## Assumptions - read before voting")
    L.append("")
    L.append(f"1. Pool fees are extrapolated linearly from {meta['hours']:.1f}h "
             f"to the full 168h epoch. Fee flow is lumpy in reality.")
    L.append("2. Bribes are counted as-is, not extrapolated (they arrive as "
             "deposits). More may be added before epoch end - upside not "
             "modeled.")
    L.append("3. Competitor votes are frozen at current levels. Late-week "
             "voters WILL dilute these yields somewhat; once the archive "
             "spans a few epochs we can measure typical late-week vote "
             "inflow and discount for it.")
    L.append("4. Pools whose rewards are partly in unpriced tokens are "
             "valued conservatively (unpriced portion counted as zero).")
    L.append("5. Best run close to epoch end (Wednesday evening UTC) when "
             "the projection window is shortest.")
    L.append("")
    L.append("---")
    L.append("*Decision-support computation, not financial advice. "
             "Projections are estimates; verify pool safety and vote at "
             "your own judgment.*")

    report = "\n".join(L)
    path = os.path.join(OUT_DIR, "vote_plan_latest.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(report)
    with open(os.path.join(OUT_DIR, "history", f"vote_plan_{stamp}.md"),
              "w", encoding="utf-8") as f:
        f.write(report)
    return path


def main():
    my_total = sum(MY_VENFTS.values())
    print(f"Vote plan generator starting - {my_total:,.2f} veAERO to place.")
    pools = load_pools()
    hours, es = hours_into_epoch()
    print(f"{hours:.1f} hours into the epoch that started {es}.")
    pools = project_rewards(pools, hours)
    plan = water_fill(pools, my_total)
    baseline = baseline_single_pool(pools, my_total)
    meta = {"my_total": my_total, "hours": hours,
            "epoch_start": es.strftime("%Y-%m-%d %H:%M UTC"),
            "baseline": baseline}
    path = write_plan(plan, meta)
    total = sum(p["my_proj_usd"] for p in plan)
    print(f"Plan: {len(plan)} pools, projected epoch rewards ${total:,.2f}.")
    print(f"Report written: {path}")


if __name__ == "__main__":
    main()
