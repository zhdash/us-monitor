# -*- coding: utf-8 -*-
"""
us_monitor / current_bar.py
===========================
「当前 60 分钟 K 线（没走完也算）涨跌多少」—— 本项目两个自选框的唯一判据。

为什么不能直接读 K 线数组的最后一根
------------------------------------
2026-10-08 实测（盘前 07:30 ET，AAPL）：

    * `US_MinKService.getMinK`（新浪分钟 K 线）最后一根 = `2026-10-07 16:00`，
      收盘价 336.62 —— 也就是**上一交易日收盘**，不是盘前的实时价 336.67；
    * 换 5 分钟周期同样只到 `10-07 16:00`，说明该接口**在盘中也不会给「正在走的那根」**
      （至少盘前不会，且刷新有延迟）；
    * 而 `hq.sinajs.cn` 的实时价在 07:31 ET 就是 336.67，秒级跳动。

结论：**当前那根 bar 的收盘端必须用实时价填**，K 线数组只负责给「开、高、低、以及上一根收盘」。

60 分钟 bar 的边界（美东时间）
------------------------------
09:30 开盘，16:00 收盘 ⇒ 7 根：`10:30 / 11:30 / 12:30 / 13:30 / 14:30 / 15:30 / 16:00`
（时间戳是**该根的收盘时刻**，与新浪返回的 `d` 字段一致，已由实测确认）。

三种时段下的口径（这是本文件最重要的一张表）
---------------------------------------------------------------------------
| 时段              | 当前 bar 时间戳 | 开 (o)      | 高/低       | 基准 (昨收)      |
| 09:30–10:30 ET    | 当日 10:30      | 当日 09:30 开| 含 09:30 后  | 上一根 = 昨日 16:00 收 |
| 10:30–16:00 ET    | 当日整点+30     | 该根起始价   | 该根区间    | 上一根收盘       |
| 盘前/盘后          | 无（市场没开）    | 下一根开盘前  | —          | 最近收盘/实时价   |
---------------------------------------------------------------------------
盘前/盘后没有「正在走的 60 分钟 bar」，此时退化为**用实时价对最近一次收盘做比较**，
并在返回里如实标注 `session`，前端据此显示「盘前」而不是假装有 K 线。
"""
from __future__ import annotations

import datetime as _dt

# 美东 vs 北京：本项目按 EDT（UTC-4）处理，即北京时间 = 美东 + 12 小时。
# 美国夏令时 3 月第二个周日 ~ 11 月第一个周日；切换期 ±1 小时对「哪根 bar」影响有限，
# 且真正的判据是「实时价 vs 上一根收盘」这个相对量，所以用固定偏移可接受，并在此声明。
ET_OFFSET_HOURS = -12          # 北京 → 美东

# 一根 60 分钟 bar 的收盘时刻（美东，分钟数从 00:00 起算）
_BAR_MINUTES = (10 * 60 + 30, 11 * 60 + 30, 12 * 60 + 30, 13 * 60 + 30,
                14 * 60 + 30, 15 * 60 + 30, 16 * 60)
_OPEN_MIN = 9 * 60 + 30        # 09:30
_CLOSE_MIN = 16 * 60           # 16:00
_PRE_MIN = 4 * 60              # 04:00 盘前开始
_AFTER_MIN = 20 * 60           # 20:00 盘后结束


def et_now() -> _dt.datetime:
    """当前美东时间（naive）。用固定偏移，见文件头说明。"""
    return _dt.datetime.now() + _dt.timedelta(hours=ET_OFFSET_HOURS)


def session_of(et: _dt.datetime | None = None) -> str:
    """返回交易时段：regular / pre / after / closed。"""
    et = et or et_now()
    if et.weekday() >= 5:
        return "closed"
    m = et.hour * 60 + et.minute
    if _OPEN_MIN <= m < _CLOSE_MIN:
        return "regular"
    if _PRE_MIN <= m < _OPEN_MIN:
        return "pre"
    if _CLOSE_MIN <= m < _AFTER_MIN:
        return "after"
    return "closed"


def current_bar_label(et: _dt.datetime | None = None) -> str | None:
    """交易时段内返回当前进行中那根 bar 的收盘时刻字符串 `YYYY-MM-DD HH:MM`。

    非交易时段返回 None —— 不硬造一根不存在的 bar。
    """
    et = et or et_now()
    if session_of(et) != "regular":
        return None
    m = et.hour * 60 + et.minute
    for bm in _BAR_MINUTES:
        if m < bm:
            hh, mm = divmod(bm, 60)
            return "%s %02d:%02d" % (et.strftime("%Y-%m-%d"), hh, mm)
    return None


def resolve_current_bar(bars: list[dict], live_price: float | None,
                        et: _dt.datetime | None = None) -> dict | None:
    """把「K 线数组 + 实时价」揉成一根**当前（可能未走完）的 60 分钟 bar**。

    返回字段：
        t            该根 bar 的收盘时刻（美东）。盘前/盘后时为 None
        o / h / l    开高低（盘前模式下 o 取最近收盘、h/l 用实时价补齐）
        c            收盘端 = 实时价（没有实时价时退化为上一根收盘）
        base         涨跌幅的基准：交易时段 = 上一根收盘；盘前/盘后 = 最近收盘
        pct          涨跌幅 %，即 (c / base - 1) * 100
        chg          涨跌额
        done         该根是否已走完（交易时段内恒为 False —— 它正在走）
        session      regular / pre / after / closed
        src          收盘端价格来自哪里（live / last_close）
        et           计算时的美东时间
    数据不足时返回 None（比如 K 线为空）。
    """
    et = et or et_now()
    sess = session_of(et)
    if not bars:
        return None

    last = bars[-1]
    label = current_bar_label(et)

    if sess == "regular" and label is not None:
        # 交易时段：找到 label 对应的那一根（K 线源可能还没吐出它）
        idx = None
        for i in range(len(bars) - 1, -1, -1):
            if (bars[i].get("t") or "")[:16] == label:
                idx = i
                break
        if idx is not None:
            bar = bars[idx]
            base = bars[idx - 1]["c"] if idx > 0 else bar["o"]
            o, h, l = bar["o"], bar["h"], bar["l"]
            t = bar["t"]
            done = True                       # 该根已经在 K 线里 ⇒ 已走完（边界情形）
        else:
            # K 线源还没吐出当前这根 —— 用上一根收盘作为开盘，实时价作为现价
            base = last["c"]
            o = last["c"]
            h = l = None
            t = label
            done = False
        c = live_price if live_price is not None else last["c"]
        if h is None:
            h = max(o, c)
        else:
            h = max(h, c)
        if l is None:
            l = min(o, c)
        else:
            l = min(l, c)
    else:
        # 盘前 / 盘后 / 休市：没有正在走的 bar，用实时价对最近收盘
        # （口径 = 「距最近收盘涨跌多少」，前端会显示 session=pre，不冒充 K 线）
        base = last["c"]
        c = live_price if live_price is not None else last["c"]
        o = last["c"]
        h, l = max(o, c), min(o, c)
        t = None
        done = False

    if not base:
        return None
    return {
        "t": t,
        "o": round(o, 4), "h": round(h, 4), "l": round(l, 4), "c": round(c, 4),
        "base": round(base, 4),
        "chg": round(c - base, 4),
        "pct": round((c / base - 1) * 100, 3),
        "done": bool(done),
        "session": sess,
        "src": "live" if live_price is not None else "last_close",
        "et": et.strftime("%Y-%m-%d %H:%M"),
    }
