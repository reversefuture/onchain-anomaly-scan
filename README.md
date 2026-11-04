# onchain-anomaly-scan

每小时扫描链上异动，生成中文报告：

1. 交易所热钱包大额转账（Etherscan V2）
2. DEX 大额成交（GeckoTerminal）
3. Aave V3 大额借贷/存取（Etherscan logs）
4. **BTC / ETH Top25 持仓地址余额变化（vs 1 小时 / 24 小时）**
5. Gemini 摘要（`gemini-2.5-flash`，失败自动换 2.5-flash-lite / flash-latest）

## 数据源

| 数据 | 来源 | 说明 |
|---|---|---|
| BTC Top25 余额 | blockstream.info | 25 个地址并发查询 |
| ETH Top25 名单 | Blockscout `/api/v2/addresses?sort=balance` | 排除合约（Beacon/WETH/多签） |
| ETH 余额 | Blockscout，失败回退 Etherscan V2 | 免费额度 5 req/s，已退避重试 |
| BTC 名单 seed | `data/richlist_seed.json` | bitinfocharts/coincarp/dune 均被 Cloudflare 拦截，改用公开数据集 + 链上余额校验 |

## GitHub Secrets（必须配置）

仓库 Settings → Secrets and variables → Actions：

- `ETHERSCAN_API_KEY`：Etherscan V2 key
- `GEMINI_API_KEY`：Gemini key
- `FEISHU_WEBHOOK`：可选，飞书机器人 webhook（不配则不推送，只上传 artifact）

## 本地运行

```bash
pip install -r requirements.txt
export ETHERSCAN_API_KEY=...
export GEMINI_API_KEY=...
python src/exchange_scan.py --days 1 --no-notify
python src/anomaly_scan.py --days 1 --no-notify
```

报告在 `outputs/anomaly_<ts>/report.md`，同时写 `data/richlist_state.json` 供下次对比。

## 限制

- 首次运行没有基线，1h/24h 显示「首次运行，已记录基线」。
- BTC 名单来自公开快照 + 链上余额重排；名单本身不会每小时变化，余额每小时更新。
- 交易所扫描依赖 `config/exchange_wallets.yaml` 的地址表。
