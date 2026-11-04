"""richlist.py — BTC/ETH 前 25 大持仓地址及持仓变化。

数据源优先级：
  ETH 名单：etherscan.io/accounts（HTML，含标签）→ 本地 seed 兜底
  BTC 名单：本地 seed（由公开 top10000 数据集校验生成；bitinfocharts/coincarp 被 Cloudflare 拦截）
  余额：BTC 用 blockstream.info/api；ETH 用 blockscout（备用 Etherscan V2）

快照存 data/richlist_state.json，保留 72 小时，用于对比 1 小时 / 24 小时变化。
"""
from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
SEED_PATH = ROOT / "data" / "richlist_seed.json"
STATE_PATH = ROOT / "data" / "richlist_state.json"
PROXY = "http://127.0.0.1:7890"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def _proxies() -> dict | None:
    return {"http": PROXY, "https": PROXY} if PROXY else None


def _get(url: str, params: dict | None = None, timeout: int = 30, attempts: int = 2):
    last = None
    for _ in range(attempts):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=timeout,
                             proxies=_proxies())
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: PERF203
            last = e
            time.sleep(1.0)
    raise last


# ── 余额 ────────────────────────────────────────────────────────────────────
def btc_balance(addr: str) -> float | None:
    """Blockstream 优先；CI 上不可达时回退 mempool.space / blockchain.info。"""
    try:
        j = _get(f"https://blockstream.info/api/address/{addr}", timeout=20).json()
        s = j.get("chain_stats") or {}
        return (int(s.get("funded_txo_sum", 0)) - int(s.get("spent_txo_sum", 0))) / 1e8
    except Exception:
        pass
    try:
        j = _get(f"https://mempool.space/api/address/{addr}", timeout=20).json()
        s = j.get("chain_stats") or {}
        return (int(s.get("funded_txo_sum", 0)) - int(s.get("spent_txo_sum", 0))) / 1e8
    except Exception:
        pass
    j = _get("https://blockchain.info/balance", {"active": addr}, timeout=20).json()
    row = j.get(addr) or {}
    return int(row.get("final_balance") or 0) / 1e8


def eth_balance(addr: str, etherscan_key: str = "") -> float | None:
    """有 key 时 Etherscan 优先（CI 最稳）；否则 Blockscout。"""
    if etherscan_key:
        for attempt in range(3):
            r = _get("https://api.etherscan.io/v2/api",
                     {"chainid": 1, "module": "account", "action": "balance",
                      "address": addr, "tag": "latest", "apikey": etherscan_key}, 25).json()
            if str(r.get("status")) == "1" and str(r.get("result", "")).isdigit():
                return int(r["result"]) / 1e18
            time.sleep(0.6 + attempt * 0.4)
    j = _get(f"https://eth.blockscout.com/api/v2/addresses/{addr}", timeout=25).json()
    return int(j.get("coin_balance") or 0) / 1e18


# ── ETH Top25 ───────────────────────────────────────────────────────────────
def fetch_eth_top_etherscan(n: int = 25, pages: int = 5) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for p in range(1, pages + 1):
        t = _get("https://etherscan.io/accounts", {"p": p}, timeout=40).text
        for chunk in t.split("<tr>")[1:]:
            m = re.search(r"href='/address/(0x[0-9a-fA-F]{40})'", chunk)
            b = re.search(r">([\d,]+\.\d+)\s*ETH<", chunk)
            if not m or not b:
                continue
            if "title='Contract'" in chunk:      # 排除合约（Beacon/WETH/多签）
                continue
            addr = m.group(1)
            if addr.lower() in seen:
                continue
            seen.add(addr.lower())
            label = re.search(r"title='([^']*)&#10;\(0x", chunk)
            out.append({"address": addr, "balance": float(b.group(1).replace(",", "")),
                        "label": (label.group(1) if label else "").replace("&#39;", "'"),
                        "source": "etherscan"})
            if len(out) >= n:
                return out
    return out[:n]


def fetch_eth_top(n: int = 25) -> list[dict]:
    """Blockscout 优先（一次返回 50 个，按余额排序，排除合约）；失败回退 Etherscan HTML。"""
    try:
        j = _get("https://eth.blockscout.com/api/v2/addresses",
                 {"sort": "balance", "order": "desc"}, timeout=40).json()
        items = j.get("items") or []
        out = []
        for it in items:
            if it.get("is_contract"):
                continue
            bal = int(it.get("coin_balance") or 0) / 1e18
            if bal <= 0:
                continue
            out.append({"address": it["hash"], "balance": bal,
                        "label": it.get("ens_domain_name") or "", "source": "blockscout"})
            if len(out) >= n:
                break
        if len(out) >= n:
            return out
    except Exception as e:
        print("Blockscout 富豪榜失败，回退 Etherscan:", type(e).__name__)
    return fetch_eth_top_etherscan(n)


# ── BTC seed（公开数据集，需用链上余额校验）────────────────────────────────
BTC_DATASET = ("https://raw.githubusercontent.com/www222fff/bitcoin_toprank/"
               "main/btc_top10000.json")


def fetch_btc_candidates(limit: int = 60) -> list[str]:
    j = _get(BTC_DATASET, timeout=60).json()
    rows = j.get("result") or []
    out = []
    for row in rows[:limit]:
        if isinstance(row, dict):
            out.extend([k for k, v in row.items()
                        if re.fullmatch(r"(1|3|bc1)[a-zA-HJ-NP-Z0-9]{20,70}", k)])
    return out[:limit]


def build_seed(n: int = 25) -> dict:
    """生成 seed：BTC 从公开数据集取候选，用链上余额重新排序；ETH 直接抓 Etherscan。"""
    print("构建 seed：BTC 候选校验中…")
    btc = []
    for addr in fetch_btc_candidates(60):
        try:
            bal = btc_balance(addr)
        except Exception as e:
            print("  BTC 校验失败", addr[:12], type(e).__name__)
            continue
        if bal and bal > 0:
            btc.append({"address": addr, "balance": bal, "label": "", "source": "blockstream"})
        time.sleep(0.15)
        if len(btc) >= n:
            break
    btc.sort(key=lambda x: -x["balance"])
    eth = fetch_eth_top(n)
    seed = {"built_at": time.time(), "btc": btc[:n], "eth": eth[:n]}
    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    SEED_PATH.write_text(json.dumps(seed, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"seed 已写入 {SEED_PATH}: BTC {len(seed['btc'])} / ETH {len(seed['eth'])}")
    return seed


def load_seed() -> dict:
    if not SEED_PATH.exists():
        return build_seed()
    return json.loads(SEED_PATH.read_text(encoding="utf-8"))


# ── 快照与对比 ──────────────────────────────────────────────────────────────
def _gather(addrs: list[str], fn, workers: int = 6) -> dict:
    out: dict[str, float] = {}
    errs = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fn, a): a for a in addrs}
        for f in as_completed(futs):
            try:
                v = f.result()
            except Exception as e:
                errs += 1
                if errs <= 3:
                    print("  余额失败:", futs[f][:12], type(e).__name__, str(e)[:80])
                continue
            if v is not None:
                out[futs[f]] = v
    if errs:
        print(f"  余额汇总: 失败 {errs}/{len(addrs)}")
    return out


def btc_balances_batch(addrs: list[str]) -> dict:
    """一次请求拿全部 BTC 余额（blockchain.info 支持 | 分隔），避免 25 次并发被限流。"""
    j = _get("https://blockchain.info/balance",
             {"active": "|".join(addrs)}, timeout=45).json()
    out = {}
    for a in addrs:
        row = j.get(a) or {}
        if "final_balance" in row:
            out[a] = int(row["final_balance"]) / 1e8
    return out


def eth_balances_batch(addrs: list[str], key: str) -> dict:
    """Etherscan balancemulti，每批 20 个地址。"""
    out = {}
    for i in range(0, len(addrs), 20):
        chunk = addrs[i:i + 20]
        r = _get("https://api.etherscan.io/v2/api",
                 {"chainid": 1, "module": "account", "action": "balancemulti",
                  "address": ",".join(chunk), "tag": "latest", "apikey": key},
                 timeout=30).json()
        for row in r.get("result") or []:
            if isinstance(row, dict) and str(row.get("balance", "")).isdigit():
                out[row["account"]] = int(row["balance"]) / 1e18
    return out


def take_snapshot(seed: dict | None = None, etherscan_key: str = "",
                  refresh_eth_list: bool = True) -> dict:
    seed = seed or load_seed()
    etherscan_key = etherscan_key or os.getenv("ETHERSCAN_API_KEY", "") \
        or os.getenv("ETHERSCAN_TOKEN", "")
    if refresh_eth_list:
        try:
            eth_list = fetch_eth_top(25) or seed.get("eth") or []
        except Exception as e:
            print("ETH 富豪榜刷新失败，使用 seed:", type(e).__name__)
            eth_list = seed.get("eth") or []
    else:
        eth_list = seed.get("eth") or []

    btc_addrs = [r["address"] for r in (seed.get("btc") or [])]
    try:
        btc = {k: round(v, 8) for k, v in btc_balances_batch(btc_addrs).items()}
    except Exception as e:
        print("BTC 批量余额失败，逐个查询:", type(e).__name__)
        btc = {k: round(v, 8) for k, v in _gather(btc_addrs, btc_balance, workers=2).items()}
    eth_addrs = [r["address"] for r in eth_list]
    if etherscan_key:
        try:
            eth_raw = eth_balances_batch(eth_addrs, etherscan_key)
        except Exception as e:
            print("ETH 批量余额失败，逐个查询:", type(e).__name__)
            eth_raw = _gather(eth_addrs, lambda a: eth_balance(a, etherscan_key), workers=2)
    else:
        eth_raw = _gather(eth_addrs, lambda a: eth_balance(a, ""), workers=2)
    labels = {r["address"]: r.get("label", "") for r in eth_list}
    eth = {k: {"balance": round(v, 6), "label": labels.get(k, "")}
           for k, v in eth_raw.items()}

    snap = {"ts": time.time(), "coverage": {"btc": len(btc), "eth": len(eth)},
            "btc": {"total": round(sum(btc.values()), 8), "addresses": btc},
            "eth": {"total": round(sum(v["balance"] for v in eth.values()), 6),
                    "addresses": {k: v["balance"] for k, v in eth.items()},
                    "labels": {k: v["label"] for k, v in eth.items()}}}
    _append_state(snap)
    return snap


def _append_state(snap: dict) -> None:
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        state = {"snapshots": []}
    snaps = [s for s in state.get("snapshots", []) if time.time() - s["ts"] < 72 * 3600]
    snaps.append(snap)
    state["snapshots"] = snaps[-200:]
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _closest(target_ts: float, exclude_last: bool = True,
             max_diff: float | None = None) -> dict | None:
    try:
        snaps = json.loads(STATE_PATH.read_text(encoding="utf-8")).get("snapshots", [])
    except Exception:
        return None
    if exclude_last and snaps:
        snaps = snaps[:-1]
    if not snaps:
        return None
    best = min(snaps, key=lambda s: abs(s["ts"] - target_ts))
    if max_diff is not None and abs(best["ts"] - target_ts) > max_diff:
        return None
    return best


def compare(now: dict) -> dict:
    out = {}
    for horizon, sec, tol in (("1h", 3600, 1800), ("24h", 86400, 7200)):
        base = _closest(now["ts"] - sec, max_diff=tol)
        if not base:
            out[horizon] = None
            continue
        row = {"base_ts": base["ts"], "coins": {}}
        for coin in ("btc", "eth"):
            cur_a = now[coin]["addresses"]
            old_a = base[coin]["addresses"]
            common = [a for a in cur_a if a in old_a]
            changes = []
            for addr in common:
                bal, old = cur_a[addr], old_a[addr]
                d = bal - old
                if abs(d) > 0 and abs(d) / max(old, 1e-9) > 1e-6:
                    changes.append({"address": addr, "delta": d,
                                    "pct": (d / old * 100) if old else 0.0,
                                    "balance": bal})
            changes.sort(key=lambda x: -abs(x["delta"]))
            cur_sum = sum(cur_a[a] for a in common)
            old_sum = sum(old_a[a] for a in common)
            row["coins"][coin] = {"total": cur_sum, "old_total": old_sum,
                                  "delta": cur_sum - old_sum, "common": len(common),
                                  "changes": changes[:6]}
        out[horizon] = row
    return out
