"""
history_backfill.py  (v1.0)

Scans the Base blockchain for every vote ever cast on Aerodrome and saves
the results as one CSV file per weekly epoch, under data/history/votes/.

How it works, in plain English:
  - The Aerodrome "Voter" contract announces every vote as a public event
    on the blockchain ("Voted" when weight is added to a pool, "Abstained"
    when it is removed/reset).
  - We ask the RPC endpoints for those events in chunks of blocks, walking
    forward from Aerodrome's launch in 2023 to today.
  - Free RPCs refuse requests that are too big, so if a chunk fails we
    automatically split it in half and try again (adaptive chunking).
  - Progress is saved to a small file, so the job can stop (GitHub limits
    runs to ~6 hours) and the next run picks up exactly where it left off.
  - Votes are aggregated per (epoch, pool, voter NFT) so files stay small
    but we keep enough detail for whale-level analysis later.

Modes (set via the MODE environment variable):
  MODE=sample  -> only scan the most recent ~200,000 blocks (~4.5 days).
                  Used once to confirm everything decodes correctly.
  MODE=full    -> scan from Aerodrome's launch onward, resuming from the
                  progress file if one exists.
"""

import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from web3 import Web3
from hexbytes import HexBytes

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Same public-RPC failover philosophy as onchain_collector.py.
RPC_ENDPOINTS = [
    "https://base-rpc.publicnode.com",
    "https://mainnet.base.org",
    "https://base.llamarpc.com",
    "https://1rpc.io/base",
]

# Aerodrome Voter contract on Base (the contract every vote goes through).
VOTER_ADDRESS = "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5"

# Block to start scanning from in full mode. Aerodrome launched late August
# 2023; block 3,000,000 is comfortably before that, and empty ranges are
# cheap to scan.
START_BLOCK = 3_000_000

# Initial number of blocks to request per call. Automatically halved when an
# endpoint complains, down to MIN_CHUNK.
INITIAL_CHUNK = 10_000
MIN_CHUNK = 250

# Stop cleanly after this many minutes so GitHub can commit the results.
MAX_MINUTES = int(os.environ.get("MAX_MINUTES", "300"))

# Write results + progress to disk at least this often (minutes), so a
# crashed run loses very little.
CHECKPOINT_MINUTES = 30

OUT_DIR = os.path.join("data", "history", "votes")
PROGRESS_FILE = os.path.join(OUT_DIR, "_progress.json")

WEEK = 7 * 24 * 3600  # Epochs flip every Thursday 00:00 UTC.

# Event signatures for the Voter contract (Velodrome V2 / Aerodrome layout):
#   Voted(voter, pool, tokenId, weight, totalWeight, timestamp)
#   Abstained(voter, pool, tokenId, weight, totalWeight, timestamp)
SIG_VOTED = "Voted(address,address,uint256,uint256,uint256,uint256)"
SIG_ABSTAINED = "Abstained(address,address,uint256,uint256,uint256,uint256)"
TOPIC_VOTED = Web3.keccak(text=SIG_VOTED).hex()
TOPIC_ABSTAINED = Web3.keccak(text=SIG_ABSTAINED).hex()


def norm_topic(t):
    """Return a topic hash as a lowercase 0x-prefixed string."""
    if isinstance(t, (bytes, HexBytes)):
        t = bytes(t).hex()
    t = t.lower()
    return t if t.startswith("0x") else "0x" + t


TOPIC_VOTED = norm_topic(TOPIC_VOTED)
TOPIC_ABSTAINED = norm_topic(TOPIC_ABSTAINED)

# ---------------------------------------------------------------------------
# RPC handling with failover
# ---------------------------------------------------------------------------

_current_rpc = 0
_w3 = None


def get_w3(rotate=False):
    """Return a Web3 connection, rotating to the next endpoint on demand."""
    global _current_rpc, _w3
    if rotate or _w3 is None:
        if rotate:
            _current_rpc = (_current_rpc + 1) % len(RPC_ENDPOINTS)
        url = RPC_ENDPOINTS[_current_rpc]
        print(f"[rpc] using {url}", flush=True)
        _w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
    return _w3


def rpc_call(fn, max_attempts=8):
    """Run an RPC call, rotating endpoints and backing off on failure.

    Raises RuntimeError("too_big") straight through so the caller can split
    the block range instead of endlessly retrying.
    """
    last_err = None
    for attempt in range(max_attempts):
        try:
            return fn(get_w3())
        except Exception as e:  # noqa: BLE001 - we inspect the message below
            msg = str(e).lower()
            # Typical "range too large / too many results" complaints.
            if any(s in msg for s in ("too many", "limit", "range", "10000",
                                      "response size", "exceed", "larger")):
                raise RuntimeError("too_big") from e
            last_err = e
            get_w3(rotate=True)
            time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"all RPC attempts failed: {last_err}")


# ---------------------------------------------------------------------------
# Event decoding
# ---------------------------------------------------------------------------

def decode_event(log):
    """Turn one raw log entry into a plain dict, or None if unrecognised."""
    topics = log["topics"]
    if len(topics) != 4:
        return None
    t0 = norm_topic(topics[0])
    if t0 == TOPIC_VOTED:
        kind = "voted"
    elif t0 == TOPIC_ABSTAINED:
        kind = "abstained"
    else:
        return None

    voter = "0x" + bytes(topics[1])[-20:].hex()
    pool = "0x" + bytes(topics[2])[-20:].hex()
    token_id = int.from_bytes(bytes(topics[3]), "big")

    data = bytes(HexBytes(log["data"]))
    if len(data) < 96:
        return None
    weight = int.from_bytes(data[0:32], "big") / 1e18       # veAERO units
    total_weight = int.from_bytes(data[32:64], "big") / 1e18
    ts = int.from_bytes(data[64:96], "big")

    return {
        "kind": kind,
        "voter": voter,
        "pool": pool,
        "token_id": token_id,
        "weight": weight,
        "total_weight": total_weight,
        "ts": ts,
    }


def epoch_start(ts):
    """Weekly epoch start (Thursday 00:00 UTC) for a unix timestamp."""
    return (ts // WEEK) * WEEK


def epoch_label(ts):
    return datetime.fromtimestamp(epoch_start(ts), tz=timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Aggregation and file output
# ---------------------------------------------------------------------------

# agg[(epoch_label, pool, token_id)] -> stats dict
agg = defaultdict(lambda: {
    "voter": "",
    "voted_weight": 0.0,
    "abstained_weight": 0.0,
    "n_voted": 0,
    "n_abstained": 0,
    "last_event_ts": 0,
})

FIELDNAMES = ["epoch_start", "pool", "token_id", "voter", "voted_weight",
              "abstained_weight", "n_voted", "n_abstained", "last_event_ts"]


def add_event(ev):
    key = (epoch_label(ev["ts"]), ev["pool"], ev["token_id"])
    row = agg[key]
    row["voter"] = ev["voter"]
    row["last_event_ts"] = max(row["last_event_ts"], ev["ts"])
    if ev["kind"] == "voted":
        row["voted_weight"] += ev["weight"]
        row["n_voted"] += 1
    else:
        row["abstained_weight"] += ev["weight"]
        row["n_abstained"] += 1


def flush_to_disk(next_block):
    """Merge everything held in memory into the per-epoch CSVs, then save
    the progress marker. Safe to call repeatedly."""
    os.makedirs(OUT_DIR, exist_ok=True)

    by_epoch = defaultdict(dict)
    for (ep, pool, token_id), row in agg.items():
        by_epoch[ep][(pool, token_id)] = row

    for ep, rows in by_epoch.items():
        path = os.path.join(OUT_DIR, f"votes_epoch_{ep}.csv")

        # Merge with anything a previous run already wrote for this epoch.
        existing = {}
        if os.path.exists(path):
            with open(path, newline="") as f:
                for r in csv.DictReader(f):
                    k = (r["pool"], int(r["token_id"]))
                    existing[k] = {
                        "voter": r["voter"],
                        "voted_weight": float(r["voted_weight"]),
                        "abstained_weight": float(r["abstained_weight"]),
                        "n_voted": int(r["n_voted"]),
                        "n_abstained": int(r["n_abstained"]),
                        "last_event_ts": int(r["last_event_ts"]),
                    }

        for k, row in rows.items():
            if k in existing:
                e = existing[k]
                e["voted_weight"] += row["voted_weight"]
                e["abstained_weight"] += row["abstained_weight"]
                e["n_voted"] += row["n_voted"]
                e["n_abstained"] += row["n_abstained"]
                e["last_event_ts"] = max(e["last_event_ts"], row["last_event_ts"])
                e["voter"] = row["voter"] or e["voter"]
            else:
                existing[k] = row

        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES)
            w.writeheader()
            for (pool, token_id), e in sorted(existing.items()):
                w.writerow({
                    "epoch_start": ep,
                    "pool": pool,
                    "token_id": token_id,
                    "voter": e["voter"],
                    "voted_weight": round(e["voted_weight"], 6),
                    "abstained_weight": round(e["abstained_weight"], 6),
                    "n_voted": e["n_voted"],
                    "n_abstained": e["n_abstained"],
                    "last_event_ts": e["last_event_ts"],
                })

    agg.clear()

    with open(PROGRESS_FILE, "w") as f:
        json.dump({"next_block": next_block,
                   "updated": datetime.now(timezone.utc).isoformat()}, f)
    print(f"[checkpoint] saved through block {next_block:,} "
          f"({len(by_epoch)} epoch files touched)", flush=True)


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

def fetch_logs(from_block, to_block):
    def _do(w3):
        return w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": Web3.to_checksum_address(VOTER_ADDRESS),
            "topics": [[TOPIC_VOTED, TOPIC_ABSTAINED]],
        })
    return rpc_call(_do)


def scan_range(from_block, to_block, chunk):
    """Scan [from_block, to_block], splitting chunks when RPCs push back.
    Returns (events_processed, unknown_logs)."""
    processed = 0
    unknown = 0
    start_time = time.time()
    last_checkpoint = time.time()
    current = from_block

    while current <= to_block:
        end = min(current + chunk - 1, to_block)
        try:
            logs = fetch_logs(current, end)
        except RuntimeError as e:
            if str(e) == "too_big" and chunk > MIN_CHUNK:
                chunk = max(MIN_CHUNK, chunk // 2)
                print(f"[scan] range too big, shrinking chunk to {chunk}", flush=True)
                continue
            raise

        for log in logs:
            ev = decode_event(log)
            if ev is None:
                unknown += 1
            else:
                add_event(ev)
                processed += 1

        current = end + 1
        time.sleep(0.12)  # be polite to free endpoints

        # Occasionally grow the chunk back if things are going smoothly.
        if len(logs) < 2000 and chunk < INITIAL_CHUNK:
            chunk = min(INITIAL_CHUNK, chunk * 2)

        elapsed_min = (time.time() - start_time) / 60
        if (time.time() - last_checkpoint) / 60 >= CHECKPOINT_MINUTES:
            flush_to_disk(current)
            last_checkpoint = time.time()

        if elapsed_min >= MAX_MINUTES:
            print(f"[scan] time budget ({MAX_MINUTES} min) reached at "
                  f"block {current:,} — stopping cleanly.", flush=True)
            flush_to_disk(current)
            return processed, unknown, False

        if processed and processed % 50000 < 100:
            pct = 100 * (current - from_block) / max(1, to_block - from_block)
            print(f"[scan] block {current:,} ({pct:.1f}%) — "
                  f"{processed:,} events so far", flush=True)

    flush_to_disk(current)
    return processed, unknown, True


def main():
    mode = os.environ.get("MODE", "sample").lower()
    latest = rpc_call(lambda w3: w3.eth.block_number)
    print(f"[main] mode={mode}, latest Base block = {latest:,}", flush=True)

    if mode == "sample":
        from_block = latest - 200_000
        # Sample mode ignores/does not touch the progress file.
        global PROGRESS_FILE
        PROGRESS_FILE = os.path.join(OUT_DIR, "_progress_sample.json")
    else:
        from_block = START_BLOCK
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE) as f:
                from_block = json.load(f)["next_block"]
            print(f"[main] resuming from block {from_block:,}", flush=True)

    if from_block > latest:
        print("[main] Backfill already complete — nothing to do. ✅", flush=True)
        return

    processed, unknown, finished = scan_range(from_block, latest, INITIAL_CHUNK)

    print(f"[main] processed {processed:,} vote events "
          f"({unknown} unrecognised logs).", flush=True)
    if unknown > processed and processed == 0:
        print("[main] WARNING: nothing decoded — the event signature may be "
              "wrong. Report this output back for debugging.", flush=True)
    if finished and mode == "full":
        print("[main] 🎉 FULL BACKFILL COMPLETE — history is up to date.", flush=True)
    elif not finished:
        print("[main] Partial run saved. Run the workflow again to continue.", flush=True)


if __name__ == "__main__":
    sys.exit(main())
