"""
history_backfill.py  (v1.10)

Scans the Base blockchain for every vote ever cast on Aerodrome and saves
the results as one CSV file per weekly epoch, under data/history/votes/.

v1.10: An unbroken wall of 429 responses is recognized as daily-quota
       exhaustion (not pacing). The scan stops cleanly, saves its exact
       position, and says so — the quota renews every 24 hours and the
       next run resumes from that block.

v1.9: - When a "too many results" error names the exact block range that
        WILL work, the scan uses that suggested range directly instead of
        blindly halving its batch size seven times.
      - The batch can now shrink all the way to a single block for
        extreme vote-density moments (epoch-flip bursts).
      - If even one block ever exceeds the provider's result limit, it is
        recorded loudly in _skipped_blocks.json instead of crashing —
        no data gap can ever be silent.

v1.8: SCHEMA v2. New final_weight column: the voter's standing allocation
      for the pool at epoch close, computed last-event-wins (a Voted event
      sets it, an Abstained event zeroes it, latest timestamp rules). This
      fixes two distortions in the raw sums: cross-epoch vote resets
      showing up as negative votes (~34% of rows), and intra-epoch
      re-votes being double-counted (~0.4%). Sum columns are kept as
      activity-intensity signals. Old-format files are detected via a
      schema marker and cleared automatically on first run.

v1.7: - Rate-limit (429) responses are now classified as "throttle": the
        script waits briefly and re-asks the same endpoint instead of
        detouring through flakier ones.
      - Politeness delay between chunks raised to respect Infura's
        per-second credit throttle.

v1.6: - Fixed a misclassification: "query returned more than 10000 results
        ... try with this block range" errors were being read as a range
        POLICY refusal (endpoint blacklisted) instead of a request-too-fat
        signal (shrink the batch). Size symptoms are now checked first.
      - Batch size can now cautiously grow back after a long streak of
        successes, so one dense voting period doesn't cap throughput for
        the entire multi-year scan.

v1.5: Deep diagnostic prints the provider's full response body when the
      private endpoint fails preflight.

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
import re
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

def _normalize_private_rpc(raw):
    """Clean up the stored secret and report its shape WITHOUT revealing it.
    Returns (url, list_of_report_lines). url is "" if unusable."""
    report = []
    val = raw.strip().strip('"').strip("'").strip()
    if not val:
        return "", ["[secret-check] No PRIVATE_RPC_URL secret found — "
                    "running on public endpoints only."]

    if val.lower().startswith("http"):
        if "/v2/" in val:
            key = val.split("/v2/", 1)[1].strip("/")
            if len(key) >= 10:
                report.append(f"[secret-check] Secret is a full URL with a "
                              f"{len(key)}-character key after /v2/ — shape OK.")
            else:
                report.append("[secret-check] PROBLEM: secret is a URL that "
                              "ends at /v2/ with no API key after it. Update "
                              "the secret to include the key at the end.")
                return "", report
        else:
            report.append("[secret-check] Secret is a URL without /v2/ in it "
                          "— accepting it, but if this is Alchemy, the URL "
                          "usually contains /v2/ followed by the key.")
        if "base-mainnet" not in val and "alchemy" in val:
            report.append("[secret-check] WARNING: this Alchemy URL does not "
                          "contain 'base-mainnet' — it may point at the wrong "
                          "network. It must be the Base Mainnet endpoint.")
        return val, report

    # Not a URL. Maybe the bare API key was stored instead.
    if 10 <= len(val) <= 100 and "/" not in val and " " not in val:
        report.append(f"[secret-check] Secret looks like a bare "
                      f"{len(val)}-character API key rather than a URL — "
                      f"auto-building the Base mainnet Alchemy URL around it.")
        return "https://base-mainnet.g.alchemy.com/v2/" + val, report

    report.append(f"[secret-check] PROBLEM: secret is {len(val)} characters "
                  "and is neither a URL nor a plausible API key. Please "
                  "re-copy the endpoint URL from the Alchemy dashboard and "
                  "update the secret.")
    return "", report


PRIVATE_RPC_URL, _SECRET_REPORT = _normalize_private_rpc(
    os.environ.get("PRIVATE_RPC_URL", ""))
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
MIN_CHUNK = 1
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
# IMPORTANT: these are checked FIRST, because size errors often include
# helpful advice containing the words "block range".
SIZE_HINTS = ("more than", "response size", "too many results", "timeout",
              "timed out", "read timed out", "10000 results", "query returned",
              "-32005", "try with this block range", "log response size")


# Hints that we're being rate-limited -> wait briefly and re-ask the SAME
# endpoint. Checked before everything else.
THROTTLE_HINTS = ("429", "too many requests", "rate limit", "rate-limit",
                  "throughput")


def classify(err_msg):
    m = err_msg.lower()
    if any(h in m for h in THROTTLE_HINTS):
        return "throttle"
    if any(h in m for h in SIZE_HINTS):
        return "size"
    if any(h in m for h in RANGE_HINTS):
        return "range"
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

def diagnose_private_rpc():
    """Send two raw test requests to the private endpoint and print the
    provider's FULL response (key censored), so its own explanation of the
    problem appears in the log."""
    import requests as _rq
    print("[diagnose] Private endpoint failed preflight — sending raw test "
          "requests to capture the provider's actual explanation...",
          flush=True)
    tests = [
        ("simple (eth_blockNumber)",
         {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber",
          "params": []}),
        ("event query (tiny eth_getLogs)",
         {"jsonrpc": "2.0", "id": 2, "method": "eth_getLogs",
          "params": [{"fromBlock": hex(QUIET_BLOCK),
                      "toBlock": hex(QUIET_BLOCK + 49),
                      "address": Web3.to_checksum_address(VOTER_ADDRESS),
                      "topics": [[TOPIC_VOTED, TOPIC_ABSTAINED]]}]}),
    ]
    for name, payload in tests:
        try:
            r = _rq.post(PRIVATE_RPC_URL, json=payload, timeout=30)
            body = scrub(r.text).strip()[:400]
            print(f"[diagnose] {name}: HTTP {r.status_code} — "
                  f"response body: {body}", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[diagnose] {name}: request failed — "
                  f"{type(e).__name__}: {scrub(str(e))[:200]}", flush=True)


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
            if url == PRIVATE_RPC_URL:
                diagnose_private_rpc()

    # Most generous endpoints first.
    RPC_ENDPOINTS = sorted(RPC_ENDPOINTS, key=lambda u: -capability[u])
    _current_rpc = 0
    _w3 = None

    # Bench every endpoint that failed the preflight entirely, so scanning
    # never wastes attempts on them. (If every working endpoint later dies,
    # fetch_logs clears the bench as a true last resort.)
    BAD_LOG_RPCS.clear()
    for i, u in enumerate(RPC_ENDPOINTS):
        if capability[u] == 0:
            BAD_LOG_RPCS.add(i)
    if BAD_LOG_RPCS:
        benched = [rpc_name(RPC_ENDPOINTS[i]) for i in sorted(BAD_LOG_RPCS)]
        print(f"[preflight] Benched for this run: {benched}", flush=True)

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
# Aggregation and file output  (SCHEMA v2)
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 2
SCHEMA_MARKER = os.path.join(OUT_DIR, "_schema.json")

agg = defaultdict(lambda: {
    "voter": "",
    "final_weight": 0.0,      # standing allocation at epoch close
    "voted_weight": 0.0,      # sum of all Voted weights (activity signal)
    "abstained_weight": 0.0,  # sum of all Abstained weights (activity signal)
    "n_voted": 0,
    "n_abstained": 0,
    "last_event_ts": 0,
})

FIELDNAMES = ["epoch_start", "pool", "token_id", "voter", "final_weight",
              "voted_weight", "abstained_weight", "n_voted", "n_abstained",
              "last_event_ts"]


def ensure_schema():
    """If the output folder contains files written under an older schema,
    clear them (they are fully regenerable from chain) and stamp the folder
    with the current schema version."""
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
            path = os.path.join(OUT_DIR, name)
            if os.path.isfile(path):
                os.remove(path)
                removed += 1
        with open(SCHEMA_MARKER, "w") as f:
            json.dump({"version": SCHEMA_VERSION}, f)
        print(f"[schema] Output format changed (v{current} -> "
              f"v{SCHEMA_VERSION}); cleared {removed} old files. The scan "
              f"will regenerate everything in the new format.", flush=True)


def add_event(ev):
    key = (epoch_label(ev["ts"]), ev["pool"], ev["token_id"])
    row = agg[key]
    row["voter"] = ev["voter"]

    # Last event wins for the standing allocation. Events are processed in
    # chain order, so on equal timestamps (abstain+vote in one transaction)
    # the later log correctly overrides the earlier one.
    if ev["ts"] >= row["last_event_ts"]:
        row["final_weight"] = ev["weight"] if ev["kind"] == "voted" else 0.0
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
                        "final_weight": float(r["final_weight"]),
                        "voted_weight": float(r["voted_weight"]),
                        "abstained_weight": float(r["abstained_weight"]),
                        "n_voted": int(r["n_voted"]),
                        "n_abstained": int(r["n_abstained"]),
                        "last_event_ts": int(r["last_event_ts"]),
                    }

        for k, row in rows.items():
            if k in existing:
                e = existing[k]
                # Standing allocation: whichever side saw the later event
                # holds the truth. Run slices scan forward in block order,
                # so the newer slice wins ties.
                if row["last_event_ts"] >= e["last_event_ts"]:
                    e["final_weight"] = row["final_weight"]
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
                    "final_weight": round(e["final_weight"], 6),
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

# Matches the provider's advice inside a "too many results" error, e.g.
# "Try with this block range [0x27CB729, 0x27CB746]" /
# "'from': '0x27CB729', ..., 'to': '0x27CB746'".
_SUGGEST_RE = re.compile(
    r"'from':\s*'0x([0-9a-fA-F]+)'.*?'to':\s*'0x([0-9a-fA-F]+)'")


def fetch_logs(from_block, to_block):
    """Fetch vote events for a block range.
    Raises RuntimeError('shrink_to:N') when the provider names the exact
    range that WILL work (N = last block of that range), or plain
    RuntimeError('shrink') when it doesn't. Every underlying error is
    printed."""
    global _current_rpc
    attempts = 0
    consecutive_throttles = 0
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
            if kind == "throttle":
                consecutive_throttles += 1
                if consecutive_throttles >= 8:
                    # A wall of unbroken 429s means the DAILY quota is spent,
                    # not that we're going too fast. Stop cleanly; the quota
                    # renews every 24 hours and the run resumes from here.
                    raise RuntimeError("quota")
                time.sleep(2.0)   # breathe, then re-ask the same endpoint
            elif kind == "range":
                consecutive_throttles = 0
                BAD_LOG_RPCS.add(_current_rpc)
                get_w3(rotate=True)
            elif kind == "size":
                m = _SUGGEST_RE.search(str(e))
                if m:
                    sug_from = int(m.group(1), 16)
                    sug_to = int(m.group(2), 16)
                    if sug_from == from_block and from_block <= sug_to < to_block:
                        raise RuntimeError(f"shrink_to:{sug_to}") from e
                raise RuntimeError("shrink") from e
            else:
                get_w3(rotate=True)
                time.sleep(min(2 ** min(attempts, 3), 8))
    raise RuntimeError("all RPC attempts failed while fetching logs "
                       "(see [rpc-err] lines above for the reasons)")


def record_skipped_block(block):
    """A block whose events couldn't be fetched even one-at-a-time. Should
    never happen; if it does, we keep a loud, permanent record so no data
    gap can ever be silent."""
    path = os.path.join(OUT_DIR, "_skipped_blocks.json")
    os.makedirs(OUT_DIR, exist_ok=True)
    skipped = []
    if os.path.exists(path):
        try:
            with open(path) as f:
                skipped = json.load(f)
        except Exception:  # noqa: BLE001
            skipped = []
    skipped.append(block)
    with open(path, "w") as f:
        json.dump(skipped, f)
    print(f"[scan] ⚠️  WARNING: block {block:,} exceeded the provider's "
          f"result limit even alone — recorded in _skipped_blocks.json and "
          f"skipped. Report this!", flush=True)


def scan_range(from_block, to_block, chunk):
    processed = 0
    unknown = 0
    start_time = time.time()
    last_checkpoint = time.time()
    current = from_block
    chunk_cap = chunk    # highest batch size we currently believe is safe
    initial_cap = chunk  # ceiling the cap may recover to after clean streaks
    streak = 0           # consecutive successful fetches since last shrink
    forced_end = None    # provider-suggested exact end block, when given

    while current <= to_block:
        end = forced_end if forced_end is not None else min(current + chunk - 1, to_block)
        try:
            logs = fetch_logs(current, end)
        except RuntimeError as e:
            m = str(e)
            if m == "quota":
                print(f"[scan] 🛑 Daily API quota appears exhausted at block "
                      f"{current:,}. Progress saved — the quota renews every "
                      f"24 hours; run the workflow again tomorrow and it "
                      f"will resume from exactly here.", flush=True)
                flush_to_disk(current)
                return processed, unknown, False
            if m.startswith("shrink_to:"):
                new_end = int(m.split(":")[1])
                if forced_end is not None and new_end >= forced_end:
                    # The suggestion isn't improving; fall back to halving.
                    forced_end = None
                else:
                    forced_end = min(new_end, to_block)
                    print(f"[scan] provider suggested exact range — fetching "
                          f"{current:,} to {forced_end:,}", flush=True)
                    continue
            if m == "shrink" or m.startswith("shrink_to:"):
                forced_end = None
                if chunk > 1:
                    chunk = max(1, chunk // 2)
                    chunk_cap = chunk  # don't grow back into the same wall
                    streak = 0
                    print(f"[scan] shrinking batch to {chunk} blocks", flush=True)
                    continue
                # Even a single block overflows the provider's limit. That
                # should be physically impossible, but never lose data
                # silently: record it loudly and move on.
                record_skipped_block(current)
                current += 1
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
        forced_end = None
        streak += 1
        time.sleep(0.45)  # stay under Infura's per-second credit throttle

        # Grow the batch back cautiously within the demonstrated-safe cap...
        if len(logs) < 2000 and chunk < chunk_cap:
            chunk = min(chunk_cap, chunk * 2)
        # ...and after a long streak of clean fetches, allow the cap itself
        # to rise again — a dense voting rush shouldn't throttle the whole
        # multi-year scan.
        elif streak >= 25 and chunk == chunk_cap and chunk_cap < initial_cap:
            chunk_cap = min(initial_cap, chunk_cap * 2)
            streak = 0
            print(f"[scan] raising batch cap back to {chunk_cap} blocks "
                  f"after a clean streak", flush=True)

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

    for line in _SECRET_REPORT:
        print(line, flush=True)

    ensure_schema()

    best_range = preflight()
    if best_range == 0:
        print("[main] FATAL: no endpoint will serve log queries at any size. "
              "Report the [preflight] lines above.", flush=True)
        return 1

    latest = rpc_call(lambda w3: w3.eth.block_number)
    # Stay ~1,000 blocks (~30 min) behind the tip: public nodes are often
    # unreliable about their freshest blocks. The next run picks them up.
    scan_top = latest - 1_000
    print(f"[main] mode={mode}, latest Base block = {latest:,}, "
          f"scanning up to {scan_top:,}", flush=True)

    if mode == "sample":
        from_block = scan_top - 200_000
        PROGRESS_FILE = os.path.join(OUT_DIR, "_progress_sample.json")
    else:
        from_block = START_BLOCK
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE) as f:
                from_block = json.load(f)["next_block"]
            print(f"[main] resuming from block {from_block:,}", flush=True)

    if from_block > scan_top:
        print("[main] Backfill already complete — nothing to do. ✅", flush=True)
        return 0

    chunk = min(INITIAL_CHUNK, best_range)
    processed, unknown, finished = scan_range(from_block, scan_top, chunk)

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
