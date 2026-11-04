"""anomaly_scan.py — 链上异动汇总：交易所热钱包 + DEX 大单 + Aave 大额借贷
                     + BTC/ETH Top25 持仓变化 + Gemini 摘要

数据源：
  - Etherscan V2（交易所转账、Aave 事件日志、ETH 余额兜底）
  - GeckoTerminal（Uniswap 等 DEX 逐笔成交）
  - Blockstream（BTC 余额）、Blockscout（ETH Top25 / 余额）

输出：outputs/anomaly_<ts>/anomalies.csv、richlist.csv、report.md
用法：python src/anomaly_scan.py [--days 2] [--min-usd 1000000] [--no-notify] [--no-ai]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from Crypto.Hash import keccak

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ai_summary
import richlist

ROOT = Path(__file__).resolve().parent.parent
PROXY = os.getenv("SCAN_PROXY")
if PROXY is None:
    PROXY = "" if os.getenv("GITHUB_ACTIONS") else "http://127.0.0.1:7890"
PX = {"http": PROXY, "https": PROXY} if PROXY else None
FEISHU = os.getenv("FEISHU_WEBHOOK", "")
AAVE_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

SIGS = {
    "Borrow": "Borrow(address,address,address,uint256,uint8,uint256,uint16)",
    "Repay": "Repay(address,address,address,uint256,bool)",
    "Supply": "Supply(address,address,address,uint256,uint16)",
    "Withdraw": "Withdraw(address,address,address,uint256)",
}


def topic0(sig: str) -> str:
    h = keccak.new(digest_bits=256)
    h.update(sig.encode())
    return "0x" + h.hexdigest()


def eth_key() -> str:
    k = os.getenv("ETHERSCAN_API_KEY") or os.getenv("ETHERSCAN_TOKEN")
    if k:
        return k
    for line in (ROOT.parent / "factor_icir" / "config" / ".api_keys.env").read_text(
            encoding="utf-8").splitlines():
        if line.startswith("ETHERSCAN_TOKEN="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("ETHERSCAN_API_KEY not set")


def get(url: str, params: dict | None = None, timeout: int = 60, attempts: int = 2):
    """带一次重试的 GET；失败两次才抛出（用户规则：先重试，再记录数据源不可靠）。"""
    last = None
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, proxies=PX, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            time.sleep(1.5)
    raise last


# ── 代币元数据（符号/精度/价格），Ethplorer 缓存 ───────────────────────────────
_token_cache: dict[str, dict] = {}


def token_meta(addr: str) -> dict:
    a = addr.lower()
    if a in _token_cache:
        return _token_cache[a]
    meta = {"symbol": a[:10], "decimals": 18, "price": 0.0}
    try:
        r = get(f"https://api.ethplorer.io/getTokenInfo/{a}?apiKey=freekey", timeout=30)
        j = r.json() or {}
        meta["symbol"] = j.get("symbol") or meta["symbol"]
        meta["decimals"] = int(j.get("decimals") or 18)
        meta["price"] = float((j.get("price") or {}).get("rate") or 0)
    except Exception:
        pass
    _token_cache[a] = meta
    return meta


# ── A. 交易所热钱包大额转账（复用最近一次扫描结果）──────────────────────────
def exchange_rows(days: int, min_usd: float) -> list[dict]:
    dirs = sorted((ROOT / "outputs").glob("scan_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for d in dirs:
        f = d / "large_transfers.csv"
        if f.exists() and (time.time() - f.stat().st_mtime) < 2 * 86400:
            df = pd.read_csv(f)
            df = df[pd.to_numeric(df["usd"], errors="coerce").fillna(0) >= min_usd]
            out = []
            for _, r in df.iterrows():
                usd = float(r["usd"])
                amt = float(r.get("amount") or 0)
                px = float(r.get("price") or 0) or (usd / amt if amt else 0)
                out.append({"platform": r["exchange"], "address": str(r["counterparty"]).lower(),
                            "direction": "流出" if r["direction"] == "out" else "流入",
                            "amount_usd": usd, "price": px or None,
                            "token": r["token"], "tx": r["tx"]})
            return out
    return []


# ── B. DEX 大额成交（GeckoTerminal）──────────────────────────────────────────
def dex_rows(min_usd: float, max_pools: int = 20) -> list[dict]:
    rows: list[dict] = []
    try:
        r = get("https://api.geckoterminal.com/api/v2/networks/eth/pools", {"page": 1}, 40)
        pools = (r.json() or {}).get("data", [])
    except Exception as e:
        print("DEX 池列表失败:", e)
        return rows
    for p in pools[:max_pools]:
        pid = p["id"].split("_")[-1]
        name = (p.get("attributes") or {}).get("name", pid[:10])
        dex = ((p.get("relationships") or {}).get("dex") or {}).get("data", {}).get("id", "DEX")
        try:
            rr = get(f"https://api.geckoterminal.com/api/v2/networks/eth/pools/{pid}/trades",
                     {"trade_volume_in_usd_greater_than": max(150000, min_usd // 5)}, 40)
            trades = (rr.json() or {}).get("data", [])
        except Exception:
            continue
        for t in trades:
            a = t.get("attributes") or {}
            usd = float(a.get("volume_in_usd") or 0)
            if usd < min_usd:
                continue
            kind = a.get("kind", "")
            # 取非稳定币一侧作为计价标的
            if kind == "sell":
                px = float(a.get("price_from_in_usd") or 0)
            else:
                px = float(a.get("price_to_in_usd") or 0)
            rows.append({"platform": f"{dex} {name}", "address": (a.get("tx_from_address") or "").lower(),
                         "direction": "买入" if kind == "buy" else "卖出",
                         "amount_usd": usd, "price": px, "token": name, "tx": a.get("tx_hash")})
        time.sleep(0.25)
    return rows


# ── C. Aave V3 大额借贷/存取（Etherscan 事件日志）────────────────────────────
def _addr(word: str) -> str:
    return "0x" + word[-40:].lower()


def _words(data: str) -> list[str]:
    d = data[2:] if data.startswith("0x") else data
    return [d[i:i + 64] for i in range(0, len(d), 64)]


def aave_rows(days: int, min_usd: float, max_pages: int = 6) -> list[dict]:
    key = eth_key()
    now = datetime.now(timezone.utc)
    cur = 0
    try:
        r = get("https://api.etherscan.io/v2/api", {"chainid": 1, "module": "proxy",
                                                    "action": "eth_blockNumber", "apikey": key}, 30)
        cur = int(r.json()["result"], 16)
    except Exception:
        cur = 0
    if not cur:
        return []
    start = cur - days * 7200
    rows: list[dict] = []
    for ev, sig in SIGS.items():
        t0 = topic0(sig)
        for page in range(1, max_pages + 1):
            try:
                r = get("https://api.etherscan.io/v2/api", {
                    "chainid": 1, "module": "logs", "action": "getLogs", "address": AAVE_POOL,
                    "fromBlock": start, "toBlock": cur, "topic0": t0,
                    "page": page, "offset": 200, "apikey": key}, 90)
                res = (r.json() or {}).get("result") or []
            except Exception as e:
                print(f"Aave {ev} page{page} 失败: {type(e).__name__}")
                break
            if not isinstance(res, list) or not res:
                break
            for lg in res:
                topics, data = lg.get("topics") or [], lg.get("data") or "0x"
                w = _words(data)
                try:
                    if ev == "Borrow":
                        user = _addr(w[0]); amount = int(w[1], 16)
                    elif ev == "Repay":
                        user = _addr(topics[2]); amount = int(w[0], 16)
                    elif ev == "Supply":
                        user = _addr(w[0]); amount = int(w[1], 16)
                    else:  # Withdraw
                        user = _addr(topics[2]); amount = int(w[0], 16)
                    reserve = _addr(topics[1])
                except Exception:
                    continue
                meta = token_meta(reserve)
                amt = amount / 10 ** meta["decimals"]
                usd = amt * (meta["price"] or 0)
                if usd >= min_usd:
                    rows.append({"platform": f"Aave {ev}", "address": user,
                                 "direction": "借入" if ev == "Borrow" else ("偿还" if ev == "Repay"
                                              else ("存入" if ev == "Supply" else "取出")),
                                 "amount_usd": usd, "price": meta["price"],
                                 "token": meta["symbol"], "tx": lg.get("transactionHash")})
            if len(res) < 200:
                break
            time.sleep(0.25)
    return rows


# ── D. 钱包持仓（已移除；改为 BTC/ETH Top25 快照，见 richlist.py）────────────


def feishu(text: str) -> bool:
    try:
        r = requests.post(FEISHU, json={"msg_type": "text", "content": {"text": text}},
                          proxies=PX, timeout=20)
        print("飞书:", r.status_code, (r.json() or {}).get("code"))
        return (r.json() or {}).get("code") == 0
    except Exception as e:
        print("飞书失败:", e)
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--min-usd", type=float, default=1_000_000)
    ap.add_argument("--no-notify", action="store_true")
    ap.add_argument("--no-ai", action="store_true", help="关闭 Gemini 摘要")
    a = ap.parse_args()

    health = {}
    rows = []

    def timed(name, fn, *args):
        t0 = time.time()
        try:
            r = fn(*args)
        except Exception as e:
            health[name] = {"status": "error", "error": type(e).__name__, "elapsed_s": round(time.time() - t0, 1)}
            print(f"[数据源] {name} 失败: {type(e).__name__}")
            return []
        el = time.time() - t0
        status = "ok" if el <= 300 else "slow"
        health[name] = {"status": status, "elapsed_s": round(el, 1), "rows": len(r)}
        print(f"[数据源] {name}: {len(r)} 行, {el:.0f}s, {status}")
        if status == "slow":
            print(f"[数据源] {name} 超过 5 分钟，记为不可靠，后续需换更短周期/更高阈值/更主流币种，并寻找替代源")
        return r

    rows += timed("Etherscan 交易所转账", exchange_rows, a.days, a.min_usd)
    rows += timed("GeckoTerminal DEX", dex_rows, a.min_usd)
    rows += timed("Etherscan Aave 日志", aave_rows, a.days, a.min_usd)

    df = pd.DataFrame(rows)
    if df.empty:
        print("无异动数据，仍生成 Top25 持仓报告")
        g = pd.DataFrame(columns=["platform", "address_prefix", "address", "direction",
                                  "token", "amount_usd", "tx_count", "avg_price"])
    else:
        # 汇总：平台 + 地址 + 方向
        g = (df.groupby(["platform", "address", "direction", "token"])
               .agg(amount_usd=("amount_usd", "sum"), tx_count=("amount_usd", "size"),
                    avg_price=("price", "mean"))
               .reset_index().sort_values("amount_usd", ascending=False))
        g["address_prefix"] = g["address"].str[:12] + "…"
        g["amount_usd"] = g["amount_usd"].round(0)
        g["avg_price"] = g["avg_price"].round(4)
        g = g[["platform", "address_prefix", "address", "direction", "token",
               "amount_usd", "tx_count", "avg_price"]]

    # ④ BTC/ETH Top25 持仓快照与变化（替代原有钱包持仓）
    snap, deltas = None, {}
    t0 = time.time()
    try:
        snap = richlist.take_snapshot(etherscan_key=eth_key())
        deltas = richlist.compare(snap)
        health["Top25 持仓"] = {"status": "ok", "elapsed_s": round(time.time() - t0, 1),
                                "rows": 25, "coverage": snap.get("coverage")}
    except Exception as e:
        health["Top25 持仓"] = {"status": "error", "error": type(e).__name__,
                                "elapsed_s": round(time.time() - t0, 1)}
        print("[数据源] Top25 持仓失败:", type(e).__name__, str(e)[:120])

    rrows = []
    if snap:
        for coin in ("btc", "eth"):
            for addr, bal in snap[coin]["addresses"].items():
                d1 = next((c for c in ((deltas.get("1h") or {}).get("coins", {}).get(coin, {})
                                       .get("changes") or []) if c["address"] == addr), {})
                d24 = next((c for c in ((deltas.get("24h") or {}).get("coins", {}).get(coin, {})
                                        .get("changes") or []) if c["address"] == addr), {})
                rrows.append({"coin": coin.upper(), "address": addr,
                              "label": (snap[coin].get("labels") or {}).get(addr, ""),
                              "balance": bal,
                              "delta_1h": d1.get("delta", 0.0),
                              "delta_24h": d24.get("delta", 0.0)})
    rdf = pd.DataFrame(rrows)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = ROOT / "outputs" / f"anomaly_{ts}"
    outdir.mkdir(parents=True, exist_ok=True)
    g.to_csv(outdir / "anomalies.csv", index=False, encoding="utf-8-sig")
    if not rdf.empty:
        rdf.to_csv(outdir / "richlist.csv", index=False, encoding="utf-8-sig")

    def fmt(x: float) -> str:
        return f"${x/1e6:,.1f}M" if x >= 1e6 else f"${x/1e3:,.0f}K"

    def coin(x: float, unit: str) -> str:
        return f"{x:+,.2f} {unit}" if abs(x) < 1000 else f"{x:+,.0f} {unit}"

    lines = [f"【链上异动】{datetime.now():%Y-%m-%d %H:%M}｜近 {a.days} 天｜阈值 {fmt(a.min_usd)}", "",
             "① 异动汇总（平台 / 地址 / 方向 / 代币 / 金额 / 笔数 / 均价）",
             "注：交易所/Aave 为近 2 天全量；DEX 为 GeckoTerminal 最近成交窗口（非完整 2 天）"]
    for r in g.head(25).itertuples():
        px = f"{r.avg_price:,.4f}" if pd.notna(r.avg_price) else "—"
        lines.append(f"{r.platform} | {r.address_prefix} | {r.direction} | {r.token} | "
                     f"{fmt(r.amount_usd)} | {r.tx_count}笔 | 均价 {px}")

    lines += ["", "② BTC/ETH Top25 持仓变化（vs 1 小时 / 24 小时）"]
    if not snap:
        lines.append("快照失败，见数据源健康度")
    else:
        cov = snap.get("coverage") or {}
        lines.append(f"覆盖：BTC {cov.get('btc', 0)}/25 · ETH {cov.get('eth', 0)}/25")
        for coin_, unit in (("btc", "BTC"), ("eth", "ETH")):
            tot = snap[coin_]["total"]
            d1 = (deltas.get("1h") or {}).get("coins", {}).get(coin_)
            d24 = (deltas.get("24h") or {}).get("coins", {}).get(coin_)
            if not d1 and not d24:
                lines.append(f"{unit} Top25 合计 {tot:,.0f} {unit}｜首次运行，已记录基线")
                continue
            def chg(d):
                if not d:
                    return "无基线"
                return f"{coin(d['delta'], unit)}（{(d['delta']/d['old_total']*100) if d['old_total'] else 0:+.2f}%）"
            lines.append(f"{unit} Top25 合计 {tot:,.0f} {unit}｜1h {chg(d1)}｜24h {chg(d24)}")
            for horizon in ("1h", "24h"):
                d = (deltas.get(horizon) or {}).get("coins", {}).get(coin_)
                if not d or not d.get("changes"):
                    continue
                parts = []
                for c in d["changes"][:3]:
                    lab = (snap[coin_].get("labels") or {}).get(c["address"], "")
                    name = (lab[:14] + " ") if lab else ""
                    parts.append(f"{name}{c['address'][:8]}… {coin(c['delta'], unit)}")
                lines.append(f"  {horizon} 变动：" + "；".join(parts))

    json.dump(health, open(ROOT / "data" / "source_health.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    lines += ["", "③ 数据源健康度"]
    for k, v in health.items():
        lines.append(f"{k}: {v['status']} / {v.get('elapsed_s', '-')}s / {v.get('rows', 0)} 行")
    report = "\n".join(lines)
    if not a.no_ai:
        summary = ai_summary.summarize(report)
        if summary:
            report += "\n\n④ AI 摘要（Gemini）\n" + summary
            health["Gemini"] = {"status": "ok", "elapsed_s": "-", "rows": 1}
        else:
            health["Gemini"] = {"status": "unavailable", "elapsed_s": "-", "rows": 0}
        # 摘要后再写一次健康度
        json.dump(health, open(ROOT / "data" / "source_health.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=2)
    (outdir / "report.md").write_text(report, encoding="utf-8")
    print(report)
    if not a.no_notify:
        feishu(report)
    print("\n明细:", outdir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
