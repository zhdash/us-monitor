# -*- coding: utf-8 -*-
"""
us_monitor / server.py
======================
HTTP 服务 + API 路由 + 监控引擎。

API
    GET /                   前端页面
    GET /api/board          成交额 TOP N 榜单（含进出榜流水）
    GET /api/klines         批量 60 分钟 K 线摘要（默认取榜单内全部标的）
    GET /api/watchlist      自选框：60上涨 / 60下跌（当前 60 分钟 K 线涨跌过滤）
    GET /api/detail?symbol= 单只：K 线全文 + 实时报价
    GET /api/health         数据源健康状态（含风控冷却倒计时）
    GET /api/config         前端需要的配置

设计要点
    * 榜单与 K 线分两个接口 —— 榜单受东财风控约束只能低频；K 线不受限，
      可以让前端按 30 秒刷。分开口前端体验会好很多（先出表，再补指标）。
    * 所有异常都在服务端消化成「结构化提示」写进 note 字段，前端永远不会白屏。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from datasource import MarketData                      # noqa: E402
from current_bar import resolve_current_bar, session_of, et_now   # noqa: E402
from indicators import summarize                       # noqa: E402
from rules import default_rules, evaluate               # noqa: E402


# --------------------------------------------------------------------------- #
# 监控引擎
# --------------------------------------------------------------------------- #
class Monitor:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.md = MarketData(cfg)
        self.rules = default_rules(cfg.get("rules"))
        self.top_n = int(cfg.get("board", {}).get("top_n", 30))
        self.feed_limit = int(cfg.get("ui", {}).get("feed_limit", 40))
        self.feed: list[dict] = []
        self._prev: list[str] = []
        self._last_items: list[dict] = []
        self._kl: dict[str, dict] = {}
        self._lock = threading.Lock()

    # ---------------- 榜单 ---------------- #
    def board(self) -> dict:
        res = self.md.board(self.top_n)
        items = res.get("items") or []
        if res.get("ok") and items:
            with self._lock:
                self._diff_feed(items)
                self._last_items = items
        res["feed"] = self.feed[: self.feed_limit]
        res["rules"] = [{"id": r.id, "name": r.name, "desc": r.desc,
                         "kind": r.kind, "enabled": r.enabled}
                        for r in self.rules]
        return res

    def _diff_feed(self, items: list[dict]) -> None:
        """对比上一次快照，记录谁进榜、谁掉榜。

        这是本工具最容易被忽略但最有用的一块：榜单变动是瞬时的，
        事后翻表格根本看不出来，只能靠流水回查。
        """
        cur = [it["symbol"] for it in items]
        if not self._prev:
            self._prev = cur
            for it in items[:5]:
                self._push(it, "in", "初始化建榜", "成交额排名第 %d" % it["rank"])
            return
        prev_set, cur_set = set(self._prev), set(cur)
        by_sym = {it["symbol"]: it for it in items}
        for sym in cur:
            if sym not in prev_set:
                it = by_sym[sym]
                self._push(it, "in", "进入前 %d" % self.top_n,
                           "成交额 $%.2f 亿，排名第 %d"
                           % ((it.get("amount") or 0) / 1e8, it["rank"]))
        for sym in self._prev:
            if sym not in cur_set:
                self.feed.insert(0, {
                    "ts": time.time(), "time": time.strftime("%H:%M:%S"),
                    "symbol": sym, "name": self._name_of(sym),
                    "action": "out", "reason": "跌出前 %d" % self.top_n,
                    "detail": "本次快照已不在成交额前 %d 名" % self.top_n,
                })
        self._prev = cur
        del self.feed[self.feed_limit:]

    def _name_of(self, sym: str) -> str:
        for it in self._last_items:
            if it["symbol"] == sym:
                return it.get("name") or sym
        return sym

    def _push(self, item: dict, action: str, reason: str, detail: str) -> None:
        self.feed.insert(0, {
            "ts": time.time(), "time": time.strftime("%H:%M:%S"),
            "symbol": item["symbol"], "name": item.get("name"),
            "action": action, "reason": reason, "detail": detail,
        })

    # ---------------- K 线 ---------------- #
    def klines(self, symbols: list[str] | None = None) -> dict:
        with self._lock:
            items = list(self._last_items)
        if symbols:
            want = set(symbols)
            items = [it for it in items if it["symbol"] in want]
        if not items:
            return {"ok": False, "note": "榜单尚未就绪，暂无可取 K 线的标的", "data": {}}

        raw = self.md.klines60(items)
        bcfg = self.cfg.get("indicators", {}).get("boll", {})
        tag_rules = [r for r in self.rules if r.enabled and r.kind == "tag"]
        data: dict[str, dict] = {}
        missing: list[str] = []
        for it in items:
            kl = raw.get(it["symbol"])
            if not kl or not kl.get("bars"):
                missing.append(it["symbol"])
                continue
            summ = summarize(kl["bars"], bcfg)
            summ["klt"] = kl.get("klt", 60)
            summ["tz"] = kl.get("tz", "ET")
            summ["last_t"] = (kl["bars"][-1] or {}).get("t")
            summ["tags"] = evaluate(it, summ, tag_rules)
            summ["_bars"] = kl["bars"]        # 留给自选框复用，省一次回源
            data[it["symbol"]] = summ
        with self._lock:
            self._kl.update(data)
            all_items = list(self._last_items)
        hit = sum(1 for it in all_items if it["symbol"] in data)

        # 取不到的标的：不编造数据，只如实报告 + 保留上一次成功的结果
        note = ""
        if missing:
            note = ("%d/%d 只的 60 分钟 K 线本次未取到（K 线主源东财当前受限），"
                    "缺失标的仍显示上一次成功结果：%s"
                    % (len(missing), len(all_items), ",".join(missing[:8])))
        return {"ok": bool(data), "ts": time.time(), "count": hit,
                "total": len(all_items), "data": data, "note": note,
                "missing": missing, "cached": sorted(self._kl.keys()),
                "period": self.cfg.get("kline", {}).get("period", 60)}

    # ---------------- 自选框（清单） ---------------- #
    def watchlists(self) -> dict:
        """按「自选框」条件从成交额前 N 里筛标的。

        每个框由 config.json 的 `watchlists` 段落驱动，一条 = 一个框：

            "up60":   { "name": "60上涨", "enabled": true, "side": "up",   "thr": 0.5 }
            "down60": { "name": "60下跌", "enabled": true, "side": "down", "thr": 0.5 }

        side=up    ⇒ 当前 60 分钟涨跌幅 ≥ +thr 入框
        side=down  ⇒ 当前 60 分钟涨跌幅 ≤ -thr 入框

        「当前 60 分钟涨跌幅」的准确定义见 current_bar.py —— 核心是**用实时价补齐
        正在走（未走完）的那根 60 分钟 K 线**，而不是直接读 K 线数组最后一根
        （新浪盘前/盘中不返回进行中的那根，会退化成上一交易日收盘）。

        加一个新框 = 在 config.json 里加一行 + 这里不用改。（后续要更复杂的条件时，
        把 match 换成可注册的判定函数即可。）
        """
        with self._lock:
            items = list(self._last_items)
        wl_cfg = self.cfg.get("watchlists") or {}
        boxes = {k: v for k, v in wl_cfg.items()
                 if isinstance(v, dict) and v.get("enabled", True)
                 and not k.startswith("_comment")}
        sess = session_of()
        out = {"ok": False, "session": sess, "is_regular": sess == "regular",
               "et": et_now().strftime("%Y-%m-%d %H:%M"),
               "top_n": self.top_n, "boxes": {}, "note": "", "count": 0}
        for k in boxes:
            out["boxes"][k] = {"id": k, "name": boxes[k].get("name", k),
                               "side": boxes[k].get("side", "up"),
                               "thr": float(boxes[k].get("thr", 0.5)),
                               "items": []}

        # ⛔ 只认盘中：盘前/盘后/休市一律不加载、不刷新（连实时价和 K 线都不取）。
        #    理由（老大 2026-10-08 要求）：那两个时段的「涨跌幅」不是 60 分钟 K 线口径，
        #    会给出与盘中含义完全不同的名单，容易误读。
        if sess != "regular":
            cn = {"pre": "盘前", "after": "盘后", "closed": "休市"}.get(sess, sess)
            out["note"] = "%s不加载自选框（只在盘中 09:30–16:00 ET 有效）。" % cn
            return out
        if not items:
            out["note"] = "榜单尚未就绪，自选框暂时无数据"
            return out

        # 实时价（新浪优先，盘前/盘中/盘后都有值；腾讯兜底）
        live = self.md.quotes_now([it["symbol"] for it in items])

        # 拿 K 线：优先复用 klines() 已经缓存好的（含完整 bars），缺的补取
        with self._lock:
            cached = dict(self._kl)
        need = [it for it in items
                if not (cached.get(it["symbol"], {}) or {}).get("_bars")]
        if need:
            raw = self.md.klines60(need)
            for it in need:
                kl = raw.get(it["symbol"])
                if kl and kl.get("bars"):
                    summ = summarize(kl["bars"], self.cfg.get("indicators", {}).get("boll", {}))
                    summ["klt"] = kl.get("klt", 60)
                    summ["tz"] = kl.get("tz", "ET")
                    summ["last_t"] = kl["bars"][-1].get("t")
                    summ["_bars"] = kl["bars"]
                    with self._lock:
                        self._kl[it["symbol"]] = summ

        # 组装每只的「当前 bar」
        stock = {}
        for it in items:
            sym = it["symbol"]
            q = live.get(sym) or {}
            lp = q.get("price")
            with self._lock:
                row = self._kl.get(sym) or {}
            bars = row.get("_bars")
            if not bars:
                continue
            cur = resolve_current_bar(bars, lp)
            if not cur:
                continue
            cur["symbol"] = sym
            cur["source"] = q.get("source")
            cur["live_ts"] = q.get("ts_text")
            cur["name"] = it.get("name")
            cur["amount"] = it.get("amount")
            cur["rank"] = it.get("rank")
            cur["price"] = it.get("price")
            cur["day_pct"] = it.get("pct")
            if q.get("pct_1h") is not None:
                cur["pct_1h"] = q["pct_1h"]     # 新浪口径的「最近 1 小时涨跌幅」，仅参考
            stock[sym] = cur

        # 分框
        for k, meta in out["boxes"].items():
            side, thr = meta["side"], meta["thr"]
            hits = []
            for sym, cur in stock.items():
                p = cur["pct"]
                if side == "up" and p >= thr:
                    hits.append(cur)
                elif side == "down" and p <= -thr:
                    hits.append(cur)
            hits.sort(key=lambda r: r["pct"], reverse=(side == "up"))
            meta["items"] = hits
        out["ok"] = True
        out["count"] = len(stock)
        out["note"] = ("盘中 %s ET ｜ 成交额前 %d 内，当前 60 分钟 K 线（未走完按实时价算）涨跌幅达标"
                       % (out["et"], self.top_n))
        return out

    # ---------------- 单只详情 ---------------- #
    def detail(self, symbol: str) -> dict:
        with self._lock:
            item = next((it for it in self._last_items if it["symbol"] == symbol), None)
        market = item.get("market") if item else None
        if market is None:
            market = self._guess_market(symbol)
        bars = self.md.kline_one(symbol, market,
                                 klt=self.cfg.get("kline", {}).get("period", 60),
                                 lmt=self.cfg.get("kline", {}).get("bars", 140))
        summ = summarize((bars or {}).get("bars") or [],
                         self.cfg.get("indicators", {}).get("boll", {}))
        rt = self.md.realtime(symbol, (item or {}).get("name", ""))
        # 当前（可能未走完）的 60 分钟 bar：收盘端用实时价，供图表叠加绘制
        live = self.md.quotes_now([symbol]).get(symbol) or {}
        cur = resolve_current_bar((bars or {}).get("bars") or [],
                                  live.get("price"), et_now())
        if cur:
            cur["source"] = live.get("source")
            cur["live_ts"] = live.get("ts_text")
        return {"ok": bool(bars), "symbol": symbol, "item": item,
                "bars": (bars or {}).get("bars") or [], "summary": summ,
                "cur_bar": cur,
                "tz": (bars or {}).get("tz", "ET"),
                "kline_source": "新浪" if (bars or {}).get("tz") == "ET" else "东财",
                "realtime": rt}

    @staticmethod
    def _guess_market(symbol: str) -> int:
        """榜单还没就绪时，挨个市场试一次拿 K 线（只用于详情页）。"""
        return 105

    def health(self) -> dict:
        h = self.md.health()
        h["engine"] = {
            "top_n": self.top_n,
            "watching": len(self._prev),
            "feed": len(self.feed),
            "rules": [{"id": r.id, "enabled": r.enabled} for r in self.rules],
        }
        return h


# --------------------------------------------------------------------------- #
# HTTP 层
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    server_version = "us_monitor/1.0"
    monitor: Monitor = None                            # 由 main() 注入
    config: dict = {}

    def log_message(self, fmt, *args):                 # 静音，避免刷屏
        pass

    # -- 响应工具 -- #
    def _send(self, body: bytes, ctype: str, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionAbortedError):
            pass

    def _json(self, obj, code: int = 200):
        self._send(json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8", code)

    def do_GET(self):                                   # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)
        path = u.path
        try:
            if path in ("/", "/index.html"):
                return self._static("index.html")
            if path == "/api/board":
                return self._json(self.monitor.board())
            if path == "/api/klines":
                syms = (q.get("symbols") or [""])[0]
                return self._json(self.monitor.klines([s for s in syms.split(",") if s] or None))
            if path == "/api/watchlist":
                return self._json(self.monitor.watchlists())
            if path == "/api/detail":
                sym = (q.get("symbol") or [""])[0].strip().upper()
                if not sym:
                    return self._json({"ok": False, "note": "缺少 symbol 参数"}, 400)
                return self._json(self.monitor.detail(sym))
            if path == "/api/health":
                return self._json(self.monitor.health())
            if path == "/api/config":
                return self._json(self.config)
            if path.startswith("/static/"):
                return self._static(path[len("/static/"):])
            return self._json({"ok": False, "note": "未知路径 " + path}, 404)
        except Exception as e:                          # 兜底：任何异常都别让页面挂掉
            return self._json({"ok": False, "note": "%s: %s" % (type(e).__name__, e)}, 500)

    def _static(self, rel: str):
        rel = unquote(rel).lstrip("/")
        full = os.path.normpath(os.path.join(BASE, "static", rel))
        if not full.startswith(os.path.join(BASE, "static")) or not os.path.isfile(full):
            return self._json({"ok": False, "note": "文件不存在: " + rel}, 404)
        ctype = {"html": "text/html; charset=utf-8", "js": "application/javascript; charset=utf-8",
                 "css": "text/css; charset=utf-8", "json": "application/json; charset=utf-8",
                 "svg": "image/svg+xml"}.get(rel.rsplit(".", 1)[-1], "application/octet-stream")
        with open(full, "rb") as f:
            self._send(f.read(), ctype)


class ThreadingServer(ThreadingHTTPServer):
    """ThreadingHTTPServer 已内置 ThreadingMixIn，这里只调参数。"""
    daemon_threads = True
    allow_reuse_address = True


def load_config() -> dict:
    with open(os.path.join(BASE, "config.json"), "r", encoding="utf-8") as f:
        return json.load(f)


def main(port: int | None = None):
    cfg = load_config()
    port = int(port or os.environ.get("US_MONITOR_PORT") or cfg.get("port", 8021))
    Handler.monitor = Monitor(cfg)
    Handler.config = cfg
    srv = ThreadingServer(("127.0.0.1", port), Handler)
    print("us_monitor 已启动 -> http://127.0.0.1:%d" % port, flush=True)
    print("榜单: 成交额 TOP%d · 监控周期: %s 分钟 K 线"
          % (Handler.monitor.top_n, cfg.get("kline", {}).get("period", 60)), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("已停止", flush=True)


if __name__ == "__main__":
    main()
