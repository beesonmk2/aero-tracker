"""
history_backfill.py  (v1.3)

Scans the Base blockchain for every vote ever cast on Aerodrome and saves
the results as one CSV file per weekly epoch, under data/history/votes/.

v1.3: Support for a private RPC endpoint (a free Alchemy API key), provided
      via the PRIVATE_RPC_URL environment variable / GitHub secret. If
      present, it is tried first. The URL itself is never printed, because
      a public repo means public logs. Endpoints answering "403 Forbidden"
      are now blacklisted immediately instead of retried.

v1.2: Debugging release.
      - Every RPC error is now printed with the endpoint name and the full
        error message, so failures are diagnosable from the log.
      - New PREFLIGHT step: before scanning, each endpoint is asked to serve
        progressively smaller block ranges (10,000 -> 2,000 -> 500 -> 50) on
        a quiet range, and a capability report is printed. Endpoints are then
        used in order of generosity, and the batch size is set to what the
        best endpoint actually accepts.
      - Fixed an oscillation bug where the batch size could shrink after a
        refusal and then immediately grow back into the same refusal.

v1.1: Blacklist endpoints that refuse large ranges instead of shrinking to
      match the stingiest endpoint.

Modes (set via the MODE environment variable):
  MODE=sample  -> only scan the most recent ~200,000 blocks (~4.5 days).
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

RPC_ENDPOINTS = [
    "https://base-rpc.publicnode.com",
    "https://mainnet.base.org",
    "https://base.llamarpc.com",
    "https://1rpc.io/base",
]

# A private endpoint (e.g. Alchemy) supplied as a GitHub secret. It gets
# top priority. NEVER print this URL — it contains the API key.
PRIVATE_RPC_URL = os.environ.get("PRIVATE_RPC_URL", "").strip()
if PRIVATE_RPC_URL:
    RPC_ENDPOINTS.insert(0, PRIVATE_RPC_URL)


def rpc_name(url):
    """Safe display name for an endpoint (hides the private URL/key)."""
    return "PRIVATE-RPC (secret)" if url == PRIVATE_RPC_URL else url


def scrub(text):
    """Remove the private RPC URL (and thus the API key) from any text
    before it is printed to the public logs."""
    if PRIVATE_RPC_URL:
        return text.replace(PRIVATE_RPC_URL, "PRIVATE-RPC")
    return text

# Aerodrome Voter contract on Base (the contract every vote goes through).
VOTER_ADDRESS = "0x16613524e02ad97eDfeF371bC883F2F5d6C480A5"

# Block to start scanning from in full mode (comfortably before Aerodrome's
# late-August-2023 launch; empty ranges are cheap).
START_BLOCK = 3_000_000

# A block range from before Aerodrome existed. Used by the preflight to test
# each endpoint's range policy without triggering "too many results" errors.
QUIET_BLOCK = 3_000_000

INITIAL_CHUNK = 10_000
MIN_CHUNK = 50
PREFLIGHT_SIZES = [10_000, 2_000, 500, 50]

MAX_MINUTES = int(os.environ.get("MAX_MINUTES", "300"))
CHECKPOINT_MINUTES = 30

OUT_DIR = os.path.join("data", "history", "votes")
PROGRESS_FILE = os.path.join(OUT_DIR, "_progress.json")

WEEK = 7 * 24 * 3600  # Epochs flip every Thursday 00:00 UTC.

SIG_VOTED = "Voted(address,address,uint256,uint256,uint256,uint256)"
SIG_ABSTAINED = "Abstained(address,address,uint256,uint256,uint256,uint256)"


def norm_topic(t):
    """Return a topic hash as a lowercase 0x-prefixed string."""
    if isinstance(t, (bytes, HexBytes)):
        t = bytes(t).hex()
    t = t.lower()
    return t if t.startswith("0x") else "0x" + t


TOPIC_VOTED = norm_topic(Web3.keccak(text=SIG_VOTED).hex())
TOPIC_ABSTAINED = norm_topic(Web3.keccak(text=SIG_ABSTAINED).hex())

# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

# Hints that an endpoint won't serve us at all, or has a hard block-range
# policy -> stop asking it and use a different endpoint for log queries.
RANGE_HINTS = ("blocks range", "block range", "range of blocks",
               "limited to", "max range", "range limit",
               "403", "forbidden", "unauthorized", "401")

# Hints that this particular request was too heavy (too many matching events,
# or the endpoint timed out crunching it) -> shrink the batch and retry.
SIZE_HINTS = ("more than", "response size", "too many results", "timeout",
              "timed out", "read timed out", "10000 results", "query returned")


def classify(err_msg):
    m = err_msg.lower()
    if any(h in m for h in RANGE_HINTS):
        return "range"
    if any(h in m for h in SIZE_HINTS):
        return "size"
    return "other"


# ---------------------------------------------------------------------------
# RPC handling with failover
# ---------------------------------------------------------------------------

_current_rpc = 0
_w3 = None


def get_w3(rotate=False):
    global _current_rpc, _w3
    if rotate or _w3 is None:
        if rotate:
            _current_rpc = (_current_rpc + 1) % len(RPC_ENDPOINTS)
        url = RPC_ENDPOINTS[_current_rpc]
        _w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 45}))
    return _w3


def rpc_call(fn, max_attempts=8):
    """Simple call with rotation, used for non-log requests."""
    last_err = None
    for attempt in range(max_attempts):
        try:
            return fn(get_w3())
        except Exception as e:  # noqa: BLE001
            print(f"[rpc-err] {rpc_name(RPC_ENDPOINTS[_current_rpc])}: "
                  f"{type(e).__name__}: {scrub(str(e))[:200]}", flush=True)
            last_err = e
            get_w3(rotate=True)
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"all RPC attempts failed: {last_err}")


def raw_get_logs(w3, from_block, to_block):
    return w3.eth.get_logs({
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": Web3.to_checksum_address(VOTER_ADDRESS),
        "topics": [[TOPIC_VOTED, TOPIC_ABSTAINED]],
    })


# ---------------------------------------------------------------------------
# Preflight: learn what each endpoint will actually serve
# ---------------------------------------------------------------------------

def preflight():
    """Test every endpoint against shrinking block ranges on a quiet part of
    the chain. Prints a report card, reorders RPC_ENDPOINTS from most to
    least generous, and returns the largest range the best endpoint accepts
    (0 if nothing works at all)."""
    global RPC_ENDPOINTS, _current_rpc, _w3

    print("[preflight] Testing each endpoint's eth_getLogs range policy...",
          flush=True)
    capability = {}
    for url in RPC_ENDPOINTS:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
        cap = 0
        last_err = ""
        for size in PREFLIGHT_SIZES:
            try:
                raw_get_logs(w3, QUIET_BLOCK, QUIET_BLOCK + size - 1)
                cap = size
                break
            except Exception as e:  # noqa: BLE001
                last_err = f"{type(e).__name__}: {scrub(str(e))[:160]}"
                time.sleep(0.5)
        capability[url] = cap
        if cap:
            print(f"[preflight]   {rpc_name(url)}  ->  accepts {cap:,}-block ranges",
                  flush=True)
        else:
            print(f"[preflight]   {rpc_name(url)}  ->  UNUSABLE for logs "
                  f"(last error: {last_err})", flush=True)

    # Most generous endpoints first.
    RPC_ENDPOINTS = sorted(RPC_ENDPOINTS, key=lambda u: -capability[u])
    _current_rpc = 0
    _w3 = None
    best = capability[RPC_ENDPOINTS[0]]
    print(f"[preflight] Endpoint order is now: "
          f"{[rpc_name(u) for u in RPC_ENDPOINTS]}", flush=True)
    print(f"[preflight] Best usable range: {best:,} blocks", flush=True)
    return best


# ---------------------------------------------------------------------------
# Event decoding
# ---------------------------------------------------------------------------

def decode_event(log):
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

    return {"kind": kind, "voter": voter, "pool": pool, "token_id": token_id,
            "weight": weight, "total_weight": total_weight, "ts": ts}


def epoch_start(ts):
    return (ts // WEEK) * WEEK


def epoch_label(ts):
    return datetime.fromtimestamp(epoch_start(ts), tz=timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Aggregation and file output
# ---------------------------------------------------------------------------

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
    os.makedirs(OUT_DIR, exist_ok=True)

    by_epoch = defaultdict(dict)
    for (ep, pool, token_id), row in agg.items():
        by_epoch[ep][(pool, token_id)] = row

    for ep, rows in by_epoch.items():
        path = os.path.join(OUT_DIR, f"votes_epoch_{ep}.csv")

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

BAD_LOG_RPCS = set()


def fetch_logs(from_block, to_block):
    """Fetch vote events for a block range.
    Raises RuntimeError('shrink') when the caller should retry with a
    smaller batch. Every underlying error is printed."""
    global _current_rpc
    attempts = 0
    while attempts < 12:
        attempts += 1

        hops = 0
        while _current_rpc in BAD_LOG_RPCS and hops < len(RPC_ENDPOINTS):
            get_w3(rotate=True)
            hops += 1
        if len(BAD_LOG_RPCS) >= len(RPC_ENDPOINTS):
            BAD_LOG_RPCS.clear()
            raise RuntimeError("shrink")

        url = RPC_ENDPOINTS[_current_rpc]
        try:
            return raw_get_logs(get_w3(), from_block, to_block)
        except Exception as e:  # noqa: BLE001
            msg = f"{type(e).__name__}: {scrub(str(e))[:200]}"
            kind = classify(str(e))
            print(f"[rpc-err] {rpc_name(url)} ({kind}): {msg}", flush=True)
            if kind == "range":
                BAD_LOG_RPCS.add(_current_rpc)
                get_w3(rotate=True)
            elif kind == "size":
                raise RuntimeError("shrink") from e
            else:
                get_w3(rotate=True)
                time.sleep(min(2 ** min(attempts, 3), 8))
    raise RuntimeError("all RPC attempts failed while fetching logs "
                       "(see [rpc-err] lines above for the reasons)")


def scan_range(from_block, to_block, chunk):
    processed = 0
    unknown = 0
    start_time = time.time()
    last_checkpoint = time.time()
    current = from_block
    chunk_cap = chunk  # highest batch size we currently believe is safe

    while current <= to_block:
        end = min(current + chunk - 1, to_block)
        try:
            logs = fetch_logs(current, end)
        except RuntimeError as e:
            if str(e) == "shrink" and chunk > MIN_CHUNK:
                chunk = max(MIN_CHUNK, chunk // 2)
                chunk_cap = chunk  # don't grow back into the same wall
                print(f"[scan] shrinking batch to {chunk} blocks", flush=True)
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
        time.sleep(0.12)

        # Grow the batch back cautiously, but never beyond the cap that the
        # endpoints have demonstrated they can handle.
        if len(logs) < 2000 and chunk < chunk_cap:
            chunk = min(chunk_cap, chunk * 2)

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
    global PROGRESS_FILE
    mode = os.environ.get("MODE", "sample").lower()

    best_range = preflight()
    if best_range == 0:
        print("[main] FATAL: no endpoint will serve log queries at any size. "
              "Report the [preflight] lines above.", flush=True)
        return 1

    latest = rpc_call(lambda w3: w3.eth.block_number)
    print(f"[main] mode={mode}, latest Base block = {latest:,}", flush=True)

    if mode == "sample":
        from_block = latest - 200_000
        PROGRESS_FILE = os.path.join(OUT_DIR, "_progress_sample.json")
    else:
        from_block = START_BLOCK
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE) as f:
                from_block = json.load(f)["next_block"]
            print(f"[main] resuming from block {from_block:,}", flush=True)

    if from_block > latest:
        print("[main] Backfill already complete — nothing to do. ✅", flush=True)
        return 0

    chunk = min(INITIAL_CHUNK, best_range)
    processed, unknown, finished = scan_range(from_block, latest, chunk)

    print(f"[main] processed {processed:,} vote events "
          f"({unknown} unrecognised logs).", flush=True)
    if processed == 0 and unknown > 0:
        print("[main] WARNING: logs were returned but none decoded — the "
              "event signature may be wrong. Report this output.", flush=True)
    if finished and mode == "full":
        print("[main] 🎉 FULL BACKFILL COMPLETE — history is up to date.", flush=True)
    elif not finished:
        print("[main] Partial run saved. Run the workflow again to continue.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
