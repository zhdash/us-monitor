# -*- coding: utf-8 -*-
"""
us_monitor / indicators.py
==========================
纯本地计算的技术指标 —— 不依赖任何外部接口，因此没有限流、没有延迟、不会失效。

默认服务 60 分钟 K 线：BOLL 中轨是「60 分钟级别多空分界」，也是本工具默认监控周期下
最常被拿来当参照的那条线。
"""
from __future__ import annotations


def sma(values: list[float], n: int) -> list[float | None]:
    """简单移动平均，返回与入参等长的列表（不足 n 个的位置为 None）。"""
    out: list[float | None] = [None] * len(values)
    if n <= 0:
        return out
    run = 0.0
    for i, v in enumerate(values):
        run += v
        if i >= n:
            run -= values[i - n]
        if i >= n - 1:
            out[i] = run / n
    return out


def stdev(values: list[float], n: int) -> list[float | None]:
    """滚动总体标准差（与主流行情软件的 BOLL 口径一致）。"""
    out: list[float | None] = [None] * len(values)
    for i in range(n - 1, len(values)):
        win = values[i - n + 1: i + 1]
        mu = sum(win) / n
        out[i] = (sum((x - mu) ** 2 for x in win) / n) ** 0.5
    return out


def boll(closes: list[float], n: int = 20, k: float = 2.0) -> dict[str, list]:
    """布林带。返回 {mid, up, low}，三者与 closes 等长。"""
    mid = sma(closes, n)
    sd = stdev(closes, n)
    up, low = [], []
    for m, s in zip(mid, sd):
        if m is None or s is None:
            up.append(None)
            low.append(None)
        else:
            up.append(m + k * s)
            low.append(m - k * s)
    return {"mid": mid, "up": up, "low": low}


def atr(bars: list[dict], n: int = 14) -> list[float | None]:
    """平均真实波幅 —— 用来判断「这根 K 线算不算异动」。"""
    trs: list[float] = []
    for i, b in enumerate(bars):
        if i == 0:
            trs.append(b["h"] - b["l"])
            continue
        pc = bars[i - 1]["c"]
        trs.append(max(b["h"] - b["l"], abs(b["h"] - pc), abs(b["l"] - pc)))
    return sma(trs, n)


def bar_position(close: float, mid: float | None, up: float | None,
                 low: float | None) -> str:
    """价格相对 60 分钟布林带的位置：上轨外 / 上半区 / 中轨附近 / 下半区 / 下轨外。"""
    if mid is None or up is None or low is None:
        return "数据不足"
    if close > up:
        return "上轨外"
    if close < low:
        return "下轨外"
    if close >= mid:
        return "上轨区" if (close - mid) / max(mid, 1e-9) > 0.01 else "中轨上"
    return "下轨区" if (mid - close) / max(mid, 1e-9) > 0.01 else "中轨下"


def summarize(bars: list[dict], boll_cfg: dict | None = None) -> dict:
    """把一只标的的 K 线压缩成前端需要的几个数 —— 只算一次，避免前端重复劳动。"""
    if not bars:
        return {}
    bcfg = boll_cfg or {}
    n = bcfg.get("period", 20)
    k = bcfg.get("k", 2.0)
    closes = [b["c"] for b in bars]
    bands = boll(closes, n, k)
    last = bars[-1]
    prev = bars[-2] if len(bars) > 1 else None
    mid = bands["mid"][-1]
    up = bands["up"][-1]
    low = bands["low"][-1]

    # 当前这根（未收盘）相对上一根的涨跌 —— 用于判断是否「正在走强/走弱」
    chg_pct = None
    if prev and prev["c"]:
        chg_pct = round((last["c"] / prev["c"] - 1) * 100, 3)

    # 量能：本根成交额 / 前 20 根均值
    vols = [b["amt"] for b in bars[-21:-1] if b.get("amt")]
    vol_ratio = round(last["amt"] / (sum(vols) / len(vols)), 2) if vols and last.get("amt") else None

    return {
        "last_bar": last,
        "bars": len(bars),
        "bar_chg_pct": chg_pct,
        "mid": round(mid, 4) if mid else None,
        "up": round(up, 4) if up else None,
        "low": round(low, 4) if low else None,
        "pos": bar_position(last["c"], mid, up, low),
        "mid_gap_pct": round((last["c"] / mid - 1) * 100, 2) if mid else None,
        "vol_ratio": vol_ratio,
        "amp": last.get("amp"),
    }
