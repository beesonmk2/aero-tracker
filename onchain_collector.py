"""
Aerodrome On-Chain Collector — v1
---------------------------------
Reads ground-truth allocation data directly from Base's blockchain via
Aerodrome's official "Sugar" helper contracts (addresses from the official
velodrome-finance/sugar deployment file):

  For every pool with an active gauge, per epoch:
    * votes      - current veAERO votes directed at the pool
    * emissions  - AERO emission rate the pool is receiving (per second)
    * fees       - fee rewards deposited for voters (exact, per token)
    * bribes     - incentives deposited for voters (exact, per token)

This is the "where are rewards actually pointed" half of the dataset.
Joined with the market tracker's fee/volume data (by pool address), it lets
us measure the core inefficiency: fees moving before allocations do.

Uses only free public RPC endpoints. No accounts, no API keys.
"""

import csv
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import keccak

# ---------------------------------------------------------------- settings ---

# Official deployed addresses on Base (chain id 8453), from
# velodrome-finance/sugar deployments/base.env
LP_SUGAR = "0x69dD9db6d8f8E7d83887A704f447b1a584b599A1"
REWARDS_SUGAR = "0x1b121EfDaF4ABb8785a315C51D29BCE0552A7678"

# Free public Base RPC endpoints, tried in order per call
RPC_ENDPOINTS = [
    "https://mainnet.base.org",
    "https://base-rpc.publicnode.com",
    "https://base.llamarpc.com",
    "https://1rpc.io/base",
]

EPOCH_PAGE = 100          # pools per epochsLatest call
PAUSE = 0.25              # seconds between RPC calls
HISTORY_FILE = "onchain_history.csv"

FIELDNAMES = [
    "snapshot_utc", "epoch_start_utc", "pool_address",
    "votes_veaero", "emissions_aero_per_sec", "emissions_aero_per_day",
    "fees_rewards", "bribe_rewards",
]

# Function selectors (first 4 bytes of keccak of the signature)
SEL_COUNT = keccak(text="count()")[:4]
SEL_EPOCHS_LATEST = keccak(text="epochsLatest(uint256,uint256)")[:4]
SEL_SYMBOL = keccak(text="symbol()")[:4]
SEL_DECIMALS = keccak(text="decimals()")[:4]

# ABI type of epochsLatest return value:
# LpEpoch { ts, lp, votes, emissions, bribes[], fees[] }
LP_EPOCH_TYPE = ("(uint256,address,uint256,uint256,"
                 "(address,uint256)[],(address,uint256)[])[]")

_rpc_stats = {"calls": 0, "retries": 0, "endpoint_failures": {}}
_token_cache = {}


# ------------------------------------------------------------ rpc plumbing ---


def rpc_eth_call(to_address, data_hex):
    """JSON-RPC eth_call with endpoint rotation and retries."""
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": to_address, "data": data_hex}, "latest"],
    }).encode()

    last_err = None
    for attempt in range(2):  # two full passes over the endpoint list
        for endpoint in RPC_ENDPOINTS:
            try:
                req = urllib.request.Request(
                    endpoint, data=payload,
                    headers={"Content-Type": "application/json",
                             "User-Agent": "aero-onchain-collector/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    out = json.loads(resp.read().decode())
                if "result" in out and out["result"] not in (None, "0x"):
                    _rpc_stats["calls"] += 1
                    return bytes.fromhex(out["result"][2:])
                last_err = out.get("error", {}).get("message", "empty result")
            except Exception as e:
                last_err = str(e)
            _rpc_stats["endpoint_failures"][endpoint] = \
                _rpc_stats["endpoint_failures"].get(endpoint, 0) + 1
        _rpc_stats["retries"] += 1
        time.sleep(3)
    raise RuntimeError(f"All RPC endpoints failed: {last_err}")


def call_count(contract):
    raw = rpc_eth_call(contract, "0x" + SEL_COUNT.hex())
    return abi_decode(["uint256"], raw)[0]


def call_epochs_latest(limit, offset):
    args = abi_encode(["uint256", "uint256"], [limit, offset])
    raw = rpc_eth_call(REWARDS_SUGAR,
                       "0x" + (SEL_EPOCHS_LATEST + args).hex())
    return abi_decode([LP_EPOCH_TYPE], raw)[0]


def token_meta(address):
    """Resolve a token's symbol and decimals, with caching and fallbacks."""
    address = address.lower()
    if address in _token_cache:
        return _token_cache[address]
    symbol, decimals = address[:10], 18
    try:
        raw = rpc_eth_call(address, "0x" + SEL_DECIMALS.hex())
        decimals = abi_decode(["uint8"], raw)[0]
    except Exception:
        pass
    try:
        raw = rpc_eth_call(address, "0x" + SEL_SYMBOL.hex())
        try:
            symbol = abi_decode(["string"], raw)[0]
        except Exception:
            symbol = raw[:32].rstrip(b"\x00").decode("ascii", "ignore") or symbol
    except Exception:
        pass
    _token_cache[address] = (symbol, decimals)
    return symbol, decimals


def rewards_to_json(reward_list):
    """[(token, raw_amount), ...] -> compact JSON like {"USDC": 1234.56}."""
    out = {}
    for token, amount in reward_list:
        symbol, decimals = token_meta(token)
        human = amount / (10 ** decimals)
        out[symbol] = round(out.get(symbol, 0) + human, 6)
    return json.dumps(out, separators=(",", ":")) if out else "{}"


# ------------------------------------------------------------- main ----------


def collect():
    total_pools = call_count(LP_SUGAR)
    print(f"LpSugar reports {total_pools} total pools on Aerodrome.")
    print(f"Sweeping epoch data in pages of {EPOCH_PAGE}...")

    rows = []
    offset = 0
    gauged = 0
    while offset < total_pools:
        try:
            epochs = call_epochs_latest(EPOCH_PAGE, offset)
        except Exception as e:
            print(f"  offset {offset}: FAILED after all retries ({e}) - skipping page")
            offset += EPOCH_PAGE
            continue
        for ts, lp, votes, emissions, bribes, fees in epochs:
            gauged += 1
            per_sec = emissions / 1e18
            rows.append({
                "epoch_start_utc": datetime.fromtimestamp(
                    ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "pool_address": lp.lower(),
                "votes_veaero": round(votes / 1e18, 4),
                "emissions_aero_per_sec": round(per_sec, 8),
                "emissions_aero_per_day": round(per_sec * 86400, 2),
                "fees_rewards": rewards_to_json(fees),
                "bribe_rewards": rewards_to_json(bribes),
            })
        if offset % 1000 == 0 or offset + EPOCH_PAGE >= total_pools:
            print(f"  progress: {min(offset + EPOCH_PAGE, total_pools)}"
                  f"/{total_pools} pools scanned, {gauged} gauged so far")
        offset += EPOCH_PAGE
        time.sleep(PAUSE)

    print()
    print(f"SWEEP REPORT -> pools scanned: {total_pools} | "
          f"gauged pools captured: {gauged} | rpc calls: {_rpc_stats['calls']} | "
          f"full-cycle retries: {_rpc_stats['retries']}")
    if _rpc_stats["endpoint_failures"]:
        fails = ", ".join(f"{k.split('//')[1]}: {v}"
                          for k, v in _rpc_stats["endpoint_failures"].items())
        print(f"Endpoint failures (handled): {fails}")
    return rows


def save_history(rows, snapshot_time):
    folder = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(folder, HISTORY_FILE)
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow({"snapshot_utc": snapshot_time, **r})
    return path


def print_summary(rows):
    print()
    print("=" * 66)
    print("TOP 10 POOLS BY CURRENT veAERO VOTES")
    print("=" * 66)
    for r in sorted(rows, key=lambda r: r["votes_veaero"], reverse=True)[:10]:
        print(f"  {r['pool_address'][:14]}...  "
              f"{r['votes_veaero']:>16,.0f} votes   "
              f"{r['emissions_aero_per_day']:>12,.0f} AERO/day")
    print()


def main():
    print("Aerodrome on-chain collector v1 starting...")
    print("Reading directly from Base blockchain via public RPC.")
    rows = collect()

    if not rows:
        print()
        print("No gauge data came back. Likely causes: all public RPC")
        print("endpoints down/throttled (rare), or a contract change.")
        print("Copy this whole log back to Claude for diagnosis.")
        sys.exit(1)

    snapshot_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    path = save_history(rows, snapshot_time)
    print(f"Success! Captured {len(rows)} gauged pools at {snapshot_time} UTC.")
    print(f"Snapshot added to: {path}")
    print_summary(rows)


if __name__ == "__main__":
    main()
