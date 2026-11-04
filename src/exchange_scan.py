"""exchange_scan.py — 扫描交易所热钱包近 N 天大额转账，推送飞书

用法：
  python src/exchange_scan.py                 # 默认近 2 天、>=100 万 USD、发飞书
  python src/exchange_scan.py --days 2 --min-usd 500000
  python src/exchange_scan.py --no-notify     # 只看结果
"""
from __future__ import annotations

import argparse
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

ROOT = Path(__file__).resolve().parent.parent
UA = {"User-Agent": "Mozilla/5.0"}
STABLE = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "FDUSD", "USDE", "PYUSD", "USD0"}


def load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def read_key(cfg: dict) -> str:
    k = os.getenv("ETHERSCAN_API_KEY") or os.getenv("ETHERSCAN_TOKEN")
    if k:
        return k
    f = Path(cfg["scan"]["etherscan_key_file"])
    name = cfg["scan"].get("etherscan_key_name", "ETHERSCAN_TOKEN")
    if not f.exists():
        return ""
    for line in f.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith(name + "="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


class Etherscan:
    def __init__(self, key: str, proxy: str):
        self.key = key
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        self.base = "https://api.etherscan.io/v2/api"

    def _get(self, **params) -> list[dict]:
        params.update({"chainid": 1, "apikey": self.key})
        for attempt in range(3):
            try:
                r = requests.get(self.base, params=params, headers=UA, timeout=25,
                                 proxies=self.proxies)
                j = r.json()
                if "status" not in j and "result" in j:      # proxy 接口（eth_blockNumber）
                    return j["result"]
                if str(j.get("status")) == "1":
                    return j.get("result") or []
                if j.get("message") == "No transactions found":
                    return []
            except Exception:
                pass
            time.sleep(1.5 * (attempt + 1))
        return []

    def tokentx(self, address: str, offset: int = 1000, page: int = 1,
                startblock: int = 0) -> list[dict]:
        return self._get(module="account", action="tokentx", address=address,
                         page=page, offset=offset, sort="desc", startblock=startblock)

    def txlist(self, address: str, offset: int = 1000, page: int = 1,
               startblock: int = 0) -> list[dict]:
        return self._get(module="account", action="txlist", address=address,
                         page=page, offset=offset, sort="desc", startblock=startblock)

    def block_number(self) -> int:
        rows = self._get(module="proxy", action="eth_blockNumber")
        try:
            txt = rows if isinstance(rows, str) else str(rows)
            return int(txt, 16) if txt.startswith("0x") else 0
        except Exception:
            return 0


def token_usd(symbol: str, amount: float, prices: dict[str, float]) -> float | None:
    if symbol.upper() in STABLE:
        return amount
    p = prices.get(symbol.upper())
    return amount * p if p else None


def build_prices(proxy: str = "") -> dict[str, float]:
    """OKX 现货优先；GitHub Actions（美国 IP）常被 OKX 拒绝，回退 CoinGecko。"""
    out: dict[str, float] = {}
    px = {"http": proxy, "https": proxy} if proxy else None
    try:
        import ccxt
        ex = ccxt.okx({"enableRateLimit": True, "timeout": 15000,
                       "options": {"defaultType": "spot"},
                       **({"proxies": px} if px else {})})
        ex.load_markets()
        for sym in ["ETH", "WETH", "BTC", "WBTC", "BNB", "SOL", "XRP", "DOGE", "ADA", "LINK"]:
            try:
                t = ex.fetch_ticker(f"{sym}/USDT")
                out[sym] = float(t["last"])
            except Exception:
                continue
    except Exception:
        pass
    if len(out) < 3:      # 回退：CoinGecko（无需 key）
        try:
            r = requests.get("https://api.coingecko.com/api/v3/coins/markets",
                             params={"vs_currency": "usd", "per_page": 250},
                             timeout=30, proxies=px, headers=UA)
            for row in (r.json() or []):
                sym = str(row.get("symbol") or "").upper()
                price = row.get("current_price")
                if sym and price:
                    out.setdefault(sym, float(price))
        except Exception as e:
            print("CoinGecko 价格回退失败:", type(e).__name__)
    out["WETH"] = out.get("ETH", out.get("WETH", 0)) or out.get("WETH", 0)
    out["WBTC"] = out.get("BTC", out.get("WBTC", 0)) or out.get("WBTC", 0)
    return {k: v for k, v in out.items() if v}


def send_feishu(webhook: str, text: str, proxy: str = "") -> bool:
    """推送文本到飞书自定义机器人；返回是否成功。"""
    try:
        r = requests.post(webhook, json={"msg_type": "text", "content": {"text": text}},
                          timeout=15,
                          proxies={"http": proxy, "https": proxy} if proxy else None)
        ok = r.status_code == 200 and (r.json() or {}).get("code") == 0
        print("飞书推送:", r.status_code, (r.json() or {}).get("code"))
        return ok
    except Exception as e:
        print("飞书推送失败:", type(e).__name__, e)
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config" / "exchange_wallets.yaml"))
    ap.add_argument("--days", type=float, default=None)
    ap.add_argument("--min-usd", type=float, default=None)
    ap.add_argument("--no-notify", action="store_true")
    ap.add_argument("--state", default=str(ROOT / "data" / "seen_tx.json"),
                    help="已通知过的交易哈希，避免重复推送")
    ap.add_argument("--only", default=None, help="只扫描指定 label 或地址（调试用）")
    a = ap.parse_args()

    lock = ROOT / "data" / "scan.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    if lock.exists():
        age = time.time() - lock.stat().st_mtime
        if age < 45 * 60:
            print(f"上一轮仍在运行（{age/60:.0f} 分钟前开始），本轮跳过")
            return 0
    lock.write_text(str(datetime.now()), encoding="utf-8")
    try:
        return _run(a)
    finally:
        lock.unlink(missing_ok=True)


def _run(a) -> int:
    cfg = load_cfg(Path(a.config))
    sc = cfg["scan"]
    days = float(a.days if a.days is not None else sc.get("days", 2))
    min_usd = float(a.min_usd if a.min_usd is not None else sc.get("min_usd", 1_000_000))
    key = read_key(cfg)
    if not key:
        print("缺少 Etherscan Key")
        return 1
    proxy = os.getenv("SCAN_PROXY")
    if proxy is None:
        proxy = "" if os.getenv("GITHUB_ACTIONS") else sc.get("proxy", "")
    es = Etherscan(key, proxy)
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    blk = es.block_number()
    startblock = max(0, blk - int(days * 7200)) if blk else 0
    print(f"起始区块 {startblock}（当前 {blk}）")

    wallets = cfg["wallets"]
    addr2label = {w["address"].lower(): f"{w['exchange']}:{w['label']}" for w in wallets}
    sel = [w for w in wallets if not a.only or a.only.lower() in (w["label"].lower(), str(w["address"]).lower())]
    mins = {float(w.get("min_usd", min_usd)) for w in sel}
    lo, hi = min(mins), max(mins)
    thr_label = f"≥${lo/1e4:.0f}万" if lo == hi else f"≥${lo/1e4:.0f}万~${hi/1e4:.0f}万（按地址）"
    prices = build_prices(proxy)
    print(f"价格: ETH={prices.get('ETH')} BTC={prices.get('BTC')}")

    records: list[dict] = []
    per_wallet: list[dict] = []
    for w in wallets:
        if a.only and a.only.lower() not in (w["label"].lower(), str(w["address"]).lower()):
            continue
        addr = w["address"]
        w_min = float(w.get("min_usd", min_usd))
        n_found = 0
        # ERC20 / 原生 ETH（skip_external=true 时跳过，适合高频机器人地址）
        if not w.get("skip_external"):
            for page in range(1, int(sc.get("max_pages", 3)) + 1):
                rows = es.tokentx(addr, 1000, page, startblock)
                if not rows:
                    break
                stop = False
                for t in rows:
                    ts = int(t.get("timeStamp") or 0)
                    if ts < cutoff:
                        stop = True
                        continue
                    n_found += 1
                    dec = int(t.get("tokenDecimal") or 18)
                    amt = float(t.get("value") or 0) / (10 ** dec)
                    sym = (t.get("tokenSymbol") or "?").upper()
                    usd = token_usd(sym, amt, prices)
                    if usd is None or usd < w_min:
                        continue
                    frm, to = t["from"].lower(), t["to"].lower()
                    direction = "out" if frm == addr.lower() else "in"
                    cp = to if direction == "out" else frm
                    records.append(dict(ts=ts, exchange=w["exchange"], wallet=w["label"],
                                        direction=direction, token=sym, amount=amt, usd=usd,
                                        counterparty=cp, counterparty_label=addr2label.get(cp, ""),
                                        tx=t["hash"]))
                if stop:
                    break
                time.sleep(0.25)
            # 原生 ETH
            for t in es.txlist(addr, 1000, 1, startblock):
                ts = int(t.get("timeStamp") or 0)
                if ts < cutoff:
                    break
                n_found += 1
                amt = float(t.get("value") or 0) / 1e18
                usd = amt * prices.get("ETH", 0)
                if usd < w_min:
                    continue
                frm, to = t["from"].lower(), t["to"].lower()
                direction = "out" if frm == addr.lower() else "in"
                cp = to if direction == "out" else frm
                records.append(dict(ts=ts, exchange=w["exchange"], wallet=w["label"],
                                    direction=direction, token="ETH", amount=amt, usd=usd,
                                    counterparty=cp, counterparty_label=addr2label.get(cp, ""),
                                    tx=t["hash"]))
        # 内部交易（合约→EOA 的利润归集，普通 txlist 看不到）
        if w.get("internal"):
            for t in es._get(module="account", action="txlistinternal", address=addr,
                             page=1, offset=1000, sort="desc", startblock=startblock):
                ts = int(t.get("timeStamp") or 0)
                if ts < cutoff:
                    continue
                n_found += 1
                amt = float(t.get("value") or 0) / 1e18
                usd = amt * prices.get("ETH", 0)
                if usd < w_min:
                    continue
                frm, to = (t.get("from") or "").lower(), (t.get("to") or "").lower()
                direction = "out" if frm == addr.lower() else "in"
                cp = to if direction == "out" else frm
                records.append(dict(ts=ts, exchange=w["exchange"], wallet=w["label"],
                                    direction=direction, token="ETH(内部)", amount=amt, usd=usd,
                                    counterparty=cp, counterparty_label=addr2label.get(cp, ""),
                                    tx=t["hash"]))
        per_wallet.append(dict(exchange=w["exchange"], wallet=w["label"], address=addr,
                               transfers=n_found))
        print(f"  {w['exchange']:<9} {w['label']:<22} 近{days:g}天转账 {n_found} 条")
        time.sleep(0.25)

    # 去重：只保留没通知过的 tx
    import json
    state_path = Path(a.state)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    if state_path.exists():
        try:
            seen = set(json.loads(state_path.read_text(encoding="utf-8")))
        except Exception:
            seen = set()
    all_records = records
    new_records = [r for r in all_records if r["tx"] not in seen]

    df = pd.DataFrame(all_records)
    df_new = pd.DataFrame(new_records)
    out_dir = ROOT / "outputs" / f"scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out_dir.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df.sort_values("usd", ascending=False).to_csv(out_dir / "large_transfers.csv",
                                                      index=False, encoding="utf-8-sig")
    pd.DataFrame(per_wallet).to_csv(out_dir / "wallet_activity.csv", index=False, encoding="utf-8-sig")

    # 汇总
    print(f"\n大额转账（{thr_label}）: {len(df)} 笔（新增 {len(df_new)} 笔）")
    if df_new.empty:
        msg = (f"【热钱包大额转账】{datetime.now().strftime('%Y-%m-%d %H:%M')}｜近 {days:g} 天\n"
               f"扫描 {len(sel)} 个地址，{thr_label}的转账：0 笔")
    elif True:
        df = df_new
        by_ex = defaultdict(lambda: [0, 0.0])
        for r in df.itertuples(index=False):
            by_ex[r.exchange][0] += 1
            by_ex[r.exchange][1] += r.usd
        top = df.sort_values("usd", ascending=False).head(8)
        # 非交易所对手方（潜在黑客/中转地址）
        ext = df[(df.direction == "out") & (df.counterparty_label == "")]
        ext_agg = ext.groupby("counterparty").agg(n=("usd", "size"), usd=("usd", "sum")) \
                     .sort_values("usd", ascending=False).head(5) if len(ext) else None

        lines = [f"【热钱包大额转账】{datetime.now().strftime('%Y-%m-%d %H:%M')}｜近 {days:g} 天｜{thr_label}",
                 "  ".join(f"{k} {v[0]}笔/${v[1]/1e6:.1f}M" for k, v in by_ex.items())]
        for r in top.itertuples(index=False):
            t = datetime.fromtimestamp(r.ts).strftime("%m-%d %H:%M")
            arrow = "→" if r.direction == "out" else "←"
            lines.append(f"{t} {r.exchange} {r.direction} {r.amount:,.2f} {r.token} "
                         f"(${r.usd/1e6:.1f}M) {arrow} {r.counterparty[:10]}…")
        if ext_agg is not None:
            lines.append("非交易所对手方 TOP:")
            for addr, row in ext_agg.iterrows():
                lines.append(f"  {addr[:12]}… {row['n']}笔 ${row['usd']/1e6:.1f}M")
        msg = "\n".join(lines)

    print(msg)
    (out_dir / "report.txt").write_text(msg, encoding="utf-8")
    print(f"\n明细: {out_dir}")

    if new_records:
        try:
            state_path.write_text(json.dumps(sorted(seen | {r["tx"] for r in new_records})),
                                  encoding="utf-8")
        except Exception:
            pass

    webhook = os.getenv("FEISHU_WEBHOOK") or sc.get("feishu_webhook", "")
    if sc.get("notify", True) and not a.no_notify and webhook:
        send_feishu(webhook, msg, proxy)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
