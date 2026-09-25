# -*- coding: utf-8 -*-
"""
us_monitor / rules.py
=====================
可扩展的监控规则引擎。

设计目标：**加一条监控条件 = 往 RULES 里加一个类 + 在 default_rules() 里登记一行**，
前端和 server 都不用改。规则只负责「判断 + 给一句人话解释」，不负责取数。

规则分两类：
    kind="filter" —— 决定谁进监控池（目前只有成交额前 N，这是需求硬指标）
    kind="tag"    —— 给已入池的标的打标（异动提示），不影响进出池

ctx 是规则能看到的全部上下文：
    ctx["item"]    榜单快照字段（price/pct/amount/volume/high/low/open/prev_close...）
    ctx["kline"]   summarize() 产出的 60 分钟 K 线摘要（mid/up/low/pos/vol_ratio/...）
    ctx["engine"]  引擎自身（含入池历史等，供有状态规则使用）
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Rule:
    id: str
    name: str
    desc: str
    kind: str = "tag"              # filter | tag
    enabled: bool = False
    params: dict = field(default_factory=dict)

    def check(self, ctx: dict) -> dict | None:
        """返回 None 表示不命中；返回 dict 表示命中，需含 tag/detail。"""
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# 内置规则
# --------------------------------------------------------------------------- #
class TopAmountRule(Rule):
    """成交额前 N —— 本项目唯一的 filter 规则，也是需求里的硬指标。"""

    def check(self, ctx):
        item = ctx["item"]
        n = int(self.params.get("n", 30))
        if item.get("rank") and item["rank"] <= n:
            return {"tag": "TOP%d" % n, "detail": "成交额排名第 %d" % item["rank"]}
        return None


class PctSpikeRule(Rule):
    """日内涨跌幅绝对值超过阈值 —— 最朴素的异动提示。"""

    def check(self, ctx):
        pct = ctx["item"].get("pct")
        if pct is None:
            return None
        thr = float(self.params.get("abs_pct", 5.0))
        if abs(pct) >= thr:
            return {"tag": "异动%.1f%%" % pct,
                    "detail": "日内涨跌幅 %.2f%%，超过阈值 %.1f%%" % (pct, thr)}
        return None


class AmountSurgeRule(Rule):
    """当前 60 分钟 K 线成交额 > 前 20 根均值的 N 倍 —— 量能突然放大。"""

    def check(self, ctx):
        k = ctx.get("kline") or {}
        vr = k.get("vol_ratio")
        if vr is None:
            return None
        thr = float(self.params.get("ratio", 2.0))
        if vr >= thr:
            return {"tag": "量能%.1fx" % vr,
                    "detail": "本根 60 分钟成交额是前 20 根均值的 %.2f 倍" % vr}
        return None


class BollBreakRule(Rule):
    """价格相对 60 分钟布林带的位置（默认看中轨）。"""

    def check(self, ctx):
        k = ctx.get("kline") or {}
        pos = k.get("pos")
        if not pos or k.get("mid_gap_pct") is None:
            return None
        side = self.params.get("side", "mid")
        gap = k["mid_gap_pct"]
        if side == "mid" and abs(gap) <= 0.15:
            return {"tag": "贴中轨", "detail": "现价距 60 分钟布林中轨 %.2f%%" % gap}
        if side == "break_any" and pos in ("上轨外", "下轨外"):
            return {"tag": pos, "detail": "现价已到布林%s" % pos}
        return None


# --------------------------------------------------------------------------- #
# 留给你后续扩展的位置 —— 照抄上面任意一个类的写法即可
# --------------------------------------------------------------------------- #
# class MaCrossRule(Rule):
#     """60 分钟 MA5 上穿 MA10（金叉）"""
#     def check(self, ctx):
#         ...
#
# class GapUpRule(Rule):
#     """今开相对昨收跳空超过 N%"""
#     def check(self, ctx):
#         it = ctx["item"]
#         if not it.get("open") or not it.get("prev_close"):
#             return None
#         gap = (it["open"] / it["prev_close"] - 1) * 100
#         ...
#
# class VolumeRankRiseRule(Rule):
#     """排名较上次快照上升超过 N 位（需要引擎保存历史，见 ctx['engine']）"""
#     def check(self, ctx):
#         ...


# --------------------------------------------------------------------------- #
# 注册表
# --------------------------------------------------------------------------- #
_CLASSES = {
    "top_amount": TopAmountRule,
    "pct_spike": PctSpikeRule,
    "amount_surge": AmountSurgeRule,
    "boll_break": BollBreakRule,
}

_META = {
    "top_amount": ("成交额前 N", "成交额排名进入前 N 名即入池", "filter"),
    "pct_spike": ("涨跌幅异动", "日内涨跌幅绝对值 ≥ 阈值", "tag"),
    "amount_surge": ("量能放大", "当前 60 分钟成交额 ≥ 前 20 根均值的 N 倍", "tag"),
    "boll_break": ("布林位置", "现价贴近/突破 60 分钟布林中轨", "tag"),
}


def default_rules(cfg: dict) -> list[Rule]:
    """按 config.json 的 rules 段落装配规则；未登记的 id 直接忽略（容错）。"""
    out: list[Rule] = []
    for rid, params in (cfg or {}).items():
        cls = _CLASSES.get(rid)
        if not cls:
            continue
        name, desc, kind = _META.get(rid, (rid, "", "tag"))
        p = dict(params or {})
        enabled = bool(p.pop("enabled", False))
        out.append(cls(id=rid, name=name, desc=desc, kind=kind, enabled=enabled, params=p))
    return out


def evaluate(item: dict, kline: dict | None, rules: list[Rule]) -> list[dict]:
    """对单个标的跑一遍所有 tag 规则，返回标签列表。"""
    ctx = {"item": item, "kline": kline or {}, "engine": None}
    tags: list[dict] = []
    for r in rules:
        if not r.enabled or r.kind != "tag":
            continue
        try:
            hit = r.check(ctx)
        except Exception:
            continue
        if hit:
            tags.append({"rule": r.id, "name": r.name, **hit})
    return tags
