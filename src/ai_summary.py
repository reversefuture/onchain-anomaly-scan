"""ai_summary.py — 用 Gemini 给链上扫描报告做中文摘要。

环境变量：
  GEMINI_API_KEY   必填
  GEMINI_MODEL     可选，默认 gemini-2.5-flash
  HTTPS_PROXY      本地需要时设置
"""
from __future__ import annotations

import os
import time

import requests

DEFAULT_MODEL = "gemini-2.5-flash"


def _proxies():
    p = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") or ""
    return {"http": p, "https": p} if p else None


def summarize(report: str, api_key: str = "", model: str = "",
              timeout: int = 90) -> str:
    key = api_key or os.getenv("GEMINI_API_KEY", "")
    if not key:
        return ""
    model = model or os.getenv("GEMINI_MODEL", DEFAULT_MODEL)
    prompt = (
        "你是加密市场链上分析师。下面是一份自动生成的链上异动扫描报告。\n"
        "请用中文写 200 字以内摘要，只基于报告里的数据，不要编造：\n"
        "1) 最值得关注的 3 条（按重要性排序，带具体数字）；\n"
        "2) BTC/ETH 巨鲸持仓变化说明什么（增持/减持/交易所流入流出）；\n"
        "3) 一句风险提示。\n\n"
        f"报告：\n{report[:12000]}"
    )
    try:
        models = [model]
        for m in ("gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-flash-latest"):
            if m not in models:
                models.append(m)
        last = None
        for m in models:
            for attempt in range(2):
                r = requests.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent",
                    params={"key": key},
                    json={"contents": [{"parts": [{"text": prompt}]}],
                          "generationConfig": {"temperature": 0.3, "maxOutputTokens": 900}},
                    timeout=timeout, proxies=_proxies())
                if r.status_code in (429, 500, 503):
                    last = r
                    time.sleep(2 + attempt * 2)
                    continue
                r.raise_for_status()
                data = r.json()
                parts = (data.get("candidates") or [{}])[0].get("content", {}).get("parts") or []
                return "".join(p.get("text", "") for p in parts).strip()
        if last is not None:
            last.raise_for_status()
        return ""
    except Exception as e:
        print("Gemini 摘要失败:", type(e).__name__, str(e)[:150])
        return ""
