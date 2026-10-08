# -*- coding: utf-8 -*-
"""
us_monitor / datasource.py
==========================
统一数据源层：把多个「免费、无需 API Key」的美股行情源封装成同一套接口。

对外提供三类能力
    1. board(top_n)        -> 全市场「成交额」降序榜单（覆盖 NASDAQ + NYSE + AMEX，含 ETF）
    2. klines60(symbols)   -> 批量 60 分钟 K 线（含成交额 / 振幅，供技术指标使用）
    3. realtime(symbols)   -> 实时/盘前盘后报价（交叉验证 + 详情页补充）

内置三重保护（这是本项目能长期跑下去的关键）
    * 缓存       —— 各接口各带 TTL，前端刷新再快也打不穿到上游
    * 最小间隔闸 —— 对高风控接口（东财 clist）强制最小请求间隔
    * 指数退避   —— 一旦被风控，本进程自动冷却，且继续吐「上一次成功的数据」而不是报错

数据源健康状态可通过 health() 拿到，前端会把它显示出来。
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from universe import BUILTIN_UNIVERSE
from current_bar import session_of


def _in_regular_session() -> bool:
    """当前是否在**盘中**（09:30–16:00 ET，工作日）。

    老大 2026-10-08 定调：本工具只认盘中数据 —— 盘前/盘后/休市时价格一律钉在
    最近一次盘中收盘，不做任何盘前/盘后的刷新（见 `enrich`）。
    """
    return session_of() == "regular"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

UA_DESKTOP = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
class SourceError(Exception):
    """任何上游取数失败都归一到这个异常，便于上层统一降级。"""


def http_get(url: str, headers: dict | None = None, timeout: int = 15,
             encoding: str = "utf-8") -> str:
    """带『绕过系统代理』的 GET。

    注意 1：本机系统代理会让部分行情域名直接 502，所以必须显式清空 ProxyHandler。
    注意 2：腾讯行情返回的是 GBK 编码，所以编码要可指定。
    """
    hdr = {"User-Agent": UA_DESKTOP, "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9"}
    if headers:
        hdr.update(headers)
    req = urllib.request.Request(url, headers=hdr)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:                     # noqa: PERF203
        raise SourceError(f"HTTP {e.code}") from e
    except Exception as e:                                   # 连接被重置 / 超时 / DNS
        raise SourceError(f"{type(e).__name__}: {e}") from e
    return raw.decode(encoding, "ignore")


class _Cache:
    """线程安全的内存缓存，key -> (写入时间戳, 值)。"""

    def __init__(self) -> None:
        self._data: dict[str, tuple[float, object]] = {}
        self._lock = threading.Lock()

    def fresh(self, key: str, ttl: float):
        with self._lock:
            item = self._data.get(key)
        if not item:
            return None
        ts, val = item
        return val if (time.time() - ts) <= ttl else None

    def stale(self, key: str):
        """返回 (时间戳, 值)，不管是否过期 —— 降级时用它兜底。"""
        with self._lock:
            return self._data.get(key)

    def put(self, key: str, val):
        with self._lock:
            self._data[key] = (time.time(), val)
        return val


class _Gate:
    """最小间隔闸门 + 连续失败指数退避。

    被上游风控后，调用方拿到的不是异常，而是「闸门还关着」的提示，
    由上层决定是否用缓存兜底 —— 保证前端永远有东西可看。
    """

    def __init__(self, min_interval: float, backoff: tuple[int, ...] = (60, 120, 300, 300, 300)):
        """退避封顶在 300 秒 —— 风控解除后最多 5 分钟就能自己恢复，不用手动重启。"""
        self.min_interval = min_interval
        self.backoff = backoff
        self._last = 0.0
        self._until = 0.0
        self._fails = 0
        self._lock = threading.Lock()

    def cooldown_left(self) -> float:
        with self._lock:
            return max(0.0, self._until - time.time())

    def acquire(self) -> None:
        """按最小间隔节流；若处于冷却期则等待（最多等 25 秒，避免拖死请求线程）。"""
        with self._lock:
            now = time.time()
            target = max(self._last + self.min_interval, self._until)
            if target > now:
                time.sleep(min(target - now, 25.0))
            self._last = time.time()

    def mark_ok(self) -> None:
        with self._lock:
            self._fails = 0
            self._until = 0.0

    def mark_fail(self) -> float:
        with self._lock:
            self._fails += 1
            wait = self.backoff[min(self._fails - 1, len(self.backoff) - 1)]
            self._until = time.time() + wait
            return float(wait)

    @property
    def fails(self) -> int:
        with self._lock:
            return self._fails


# --------------------------------------------------------------------------- #
# 数据源 1：东方财富（主源 —— 全市场榜单 + 60 分钟 K 线）
# --------------------------------------------------------------------------- #
class EastmoneySource:
    """东方财富公开行情接口。

    为什么选它做主源：
      * 唯一一个「免费、免 Key、且在服务端就能按成交额排序」的全市场源；
        NASDAQ / NYSE / AMEX 三市合计约 1.38 万只标的（含 ETF）一次请求即可拿到 TOP N。
      * 国内直连，延迟 0.2~0.5 秒量级，不需要科学上网。
      * K 线接口（push2his 域）没有触发风控，可以放心用于 30 只标的的批量刷新。

    已知限制（必须知道）：
      * 榜单接口（push2 域）**对高频调用有 IP 级风控**，实测连续打 20+ 次后被重置连接，
        封禁持续数分钟以上。因此本类对它强制最小间隔 + 退避。
      * 非官方公开 API、无文档承诺、无 SLA，字段/参数随时可能变。
      * 行情延迟官方未承诺。实测为「准实时」（分钟级），不能当逐笔用。
    """

    NAME = "东方财富 (eastmoney)"

    BOARD_URL = ("https://push2.eastmoney.com/api/qt/clist/get"
                 "?pn=1&pz={pz}&po=1&np=1&fltt=2&invt=2&fid=f6&fs=m:105,m:106,m:107"
                 "&fields=f12,f13,f14,f2,f3,f4,f5,f6,f15,f16,f17,f18,f20,f124")
    KLINE_URL = ("https://push2his.eastmoney.com/api/qt/stock/kline/get"
                 "?secid={secid}&klt={klt}&fqt=1&lmt={lmt}&end=20500101"
                 "&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58")

    _TRUST = {"Referer": "https://quote.eastmoney.com/"}
    MARKET_NAME = {105: "NASDAQ", 106: "NYSE", 107: "AMEX"}

    def __init__(self, board_min_interval: float = 60.0):
        # 榜单接口是全场最脆的，单独给它上闸
        self._board_gate = _Gate(board_min_interval)
        self._cache = _Cache()
        self.last_board_ts = 0.0
        self.last_board_err = ""

    # ---------------- 榜单 ---------------- #
    def board(self, top_n: int = 30, ttl: float = 60.0, pool_size: int = 200) -> dict:
        """全市场成交额降序榜单，返回前 top_n 只。

        一次请求同时取回 pool_size 只（默认 200），前 top_n 只用于展示，
        整个 pool 会被上层存成「候选池」，供东财不可用时在本地重建榜单。
        """
        key = f"board:{top_n}"
        hit = self._cache.fresh(key, ttl)
        if hit is not None:
            return hit

        if self._board_gate.cooldown_left() > 0:
            return self._degrade(key, f"风控冷却中，剩余 {self._board_gate.cooldown_left():.0f}s")

        self._board_gate.acquire()
        try:
            txt = http_get(self.BOARD_URL.format(pz=max(pool_size, top_n)), self._TRUST, timeout=12)
            payload = json.loads(txt)
        except Exception as e:
            wait = self._board_gate.mark_fail()
            self.last_board_err = str(e)
            return self._degrade(key, f"请求失败({e})，退避 {wait:.0f}s")

        data = (payload or {}).get("data") or {}
        rows = data.get("diff") or []
        if not rows:
            wait = self._board_gate.mark_fail()
            self.last_board_err = "空数据"
            return self._degrade(key, f"返回空榜单，退避 {wait:.0f}s")

        self._board_gate.mark_ok()
        pool = [self._row_to_item(r) for r in rows[:pool_size]]
        items = pool[:top_n]
        for i, it in enumerate(items, 1):
            it["rank"] = i
        out = {
            "ok": True,
            "source": self.NAME,
            "ts": time.time(),
            "market_total": data.get("total"),
            "items": items,
            "pool": pool,
            "note": "",
        }
        self.last_board_ts = time.time()
        self.last_board_err = ""
        return self._cache.put(key, out)

    def _degrade(self, key: str, reason: str) -> dict:
        """降级：吐出上一次成功的榜单，并如实标注时间与原因。"""
        old = self._cache.stale(key)
        if old:
            ts, val = old
            out = dict(val)
            out["ok"] = False
            out["stale"] = True
            out["stale_age"] = round(time.time() - ts, 1)
            out["note"] = f"已降级：{reason}｜当前展示 {time.strftime('%H:%M:%S', time.localtime(ts))} 的榜单快照"
            return out
        return {"ok": False, "source": self.NAME, "stale": True, "ts": time.time(),
                "items": [], "market_total": None, "note": f"不可用：{reason}"}

    @staticmethod
    def _row_to_item(r: dict) -> dict:
        def num(v):
            return None if v in ("-", "", None) else v
        return {
            "symbol": r.get("f12"),
            "market": r.get("f13"),
            "market_name": EastmoneySource.MARKET_NAME.get(r.get("f13"), "?"),
            "name": r.get("f14"),
            "price": num(r.get("f2")),
            "pct": num(r.get("f3")),
            "chg": num(r.get("f4")),
            "volume": num(r.get("f5")),
            "amount": num(r.get("f6")),
            "high": num(r.get("f15")),
            "low": num(r.get("f16")),
            "open": num(r.get("f17")),
            "prev_close": num(r.get("f18")),
            "mktcap": num(r.get("f20")),
            "quote_ts": r.get("f124"),
        }

    # ---------------- 60 分钟 K 线 ---------------- #
    def klines(self, symbol: str, market: int, klt: int = 60, lmt: int = 140) -> dict | None:
        """单只 K 线。klt: 5/15/30/60/101(日)/102(周)/103(月)。"""
        key = f"k:{market}.{symbol}:{klt}:{lmt}"
        hit = self._cache.fresh(key, 25.0)
        if hit is not None:
            return hit
        url = self.KLINE_URL.format(secid=f"{market}.{symbol}", klt=klt, lmt=lmt)
        try:
            payload = json.loads(http_get(url, self._TRUST, timeout=12))
        except Exception:
            old = self._cache.stale(key)
            return old[1] if old else None
        data = (payload or {}).get("data")
        if not data or not data.get("klines"):
            return None
        bars = []
        for line in data["klines"]:
            p = line.split(",")
            if len(p) < 7:
                continue
            try:
                bars.append({
                    "t": p[0],                       # 该根收盘时刻（北京时间）
                    "o": float(p[1]), "c": float(p[2]),
                    "h": float(p[3]), "l": float(p[4]),
                    "v": float(p[5]), "amt": float(p[6]),
                    "amp": float(p[7]) if len(p) > 7 else None,
                })
            except ValueError:
                continue
        out = {"symbol": symbol, "market": market, "name": data.get("name"),
               "klt": klt, "bars": bars}
        return self._cache.put(key, out)

    def klines_many(self, items: list[dict], klt: int = 60, lmt: int = 140,
                    concurrency: int = 3, gap_ms: int = 0) -> dict[str, dict]:
        """批量 K 线（线程池并发 + 提交间隔）。

        60 分钟 K 线每 60 分钟才产生一根新的，所以刷新频率本身就不需要高
        （config 里默认 300 秒一轮）。gap_ms 用来把请求摊开，避免瞬时打太多。
        """
        out: dict[str, dict] = {}
        if not items:
            return out
        workers = max(1, min(concurrency, len(items)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {}
            for it in items:
                if not it.get("symbol") or not it.get("market"):
                    continue
                futs[pool.submit(self.klines, it["symbol"], it["market"], klt, lmt)] = it
                if gap_ms:
                    time.sleep(gap_ms / 1000.0)
            for fut in futs:
                it = futs[fut]
                try:
                    res = fut.result()
                except Exception:
                    res = None
                if res:
                    out[it["symbol"]] = res
        return out

    def health(self) -> dict:
        return {
            "name": self.NAME,
            "role": "主源：全市场成交额榜单 + 60分钟K线",
            "cooldown": round(self._board_gate.cooldown_left(), 1),
            "fails": self._board_gate.fails,
            "last_board": self.last_board_ts,
            "last_error": self.last_board_err,
        }


# --------------------------------------------------------------------------- #
# 数据源 2：NASDAQ 官方接口（备选 —— 实时单只报价，含盘前/盘后）
# --------------------------------------------------------------------------- #
class NasdaqSource:
    """api.nasdaq.com 官方站点接口。

    实测返回的是**实时价**（含盘前盘后时段），这是它最大的价值 —— 东财给不了盘前盘后。
    限制：只能单只查、单次 2~3 秒较慢、榜单排序参数无效、历史 K 线只到日线、
    非公开文档、需要伪装浏览器请求头。所以只用于「单只详情 / 交叉验证」。
    """

    NAME = "NASDAQ 官方 (api.nasdaq.com)"
    URL = "https://api.nasdaq.com/api/quote/{symbol}/info?assetclass={klass}"

    def __init__(self):
        self._cache = _Cache()

    @staticmethod
    def guess_class(name: str, symbol: str) -> str:
        n = (name or "").upper()
        if "ETF" in n or "ETN" in n or symbol.upper() in {"SPY", "QQQ", "IWM", "DIA", "TLT", "GLD"}:
            return "etf"
        return "stocks"

    def realtime(self, symbol: str, klass: str = "stocks") -> dict | None:
        key = f"nq:{symbol}"
        hit = self._cache.fresh(key, 15.0)
        if hit is not None:
            return hit
        hdr = {"Accept": "application/json, text/plain, */*",
               "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"}
        try:
            payload = json.loads(http_get(self.URL.format(symbol=symbol, klass=klass), hdr, timeout=20))
        except Exception:
            return None
        pd = ((payload or {}).get("data") or {}).get("primaryData") or {}
        if not pd:
            return None

        def num(v):
            if v is None:
                return None
            s = str(v).replace("$", "").replace(",", "").replace("%", "").replace("+", "").strip()
            try:
                return float(s)
            except ValueError:
                return None

        out = {
            "symbol": symbol, "source": self.NAME,
            "price": num(pd.get("lastSalePrice")),
            "pct": num(pd.get("percentageChange")),
            "chg": num(pd.get("netChange")),
            "volume": num(pd.get("volume")),
            "ts_text": pd.get("lastTradeTimestamp"),
            "extended": None,
            "ts": time.time(),
        }
        ext = ((payload or {}).get("data") or {}).get("secondaryData") or None
        if ext and ext.get("lastSalePrice"):
            out["extended"] = {"session": ext.get("lastTradeTimestamp"),
                               "price": num(ext.get("lastSalePrice")),
                               "pct": num(ext.get("percentageChange"))}
        return self._cache.put(key, out)

    def health(self) -> dict:
        return {"name": self.NAME, "role": "备选：实时单只报价（含盘前盘后）", "cooldown": 0, "fails": 0}


# --------------------------------------------------------------------------- #
# 数据源 0：腾讯行情（实时快照 —— 实测最稳的一个，承担「谁的价钱是最新的」）
# --------------------------------------------------------------------------- #
class TencentSource:
    """qt.gtimg.cn 行情快照。

    实测数据（2026-09-24 测定）：
        * 批量 400 只标的，单次请求 0.13 秒返回，解析出 400 条；
        * 连续调用十余次没有任何风控迹象（与东财形成鲜明对比）；
        * 返回体自带【成交额】字段，不需要自己拿 成交量 × 均价 去估。

    限制：
        * **没有美股分钟级 K 线** —— `mkline` / `UsKlineController` 只覆盖 A 股和港股，
          传 m60 直接报 param error。日线可用（fqkline）。
        * 返回体是 GBK 编码、`~` 分隔的裸字符串，没有 schema，字段位置靠约定。
        * 非公开文档；美股只能在盘中/盘后拿到有效数据（盘前为空值）。
    """

    NAME = "腾讯行情 (qt.gtimg.cn)"
    SNAP_URL = "http://qt.gtimg.cn/q={symbols}"
    FQK_URL = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               "?param={sym},{period},,,{n},qfq")

    def __init__(self, batch: int = 200):
        self.batch = batch
        self._cache = _Cache()

    # ---------- 批量快照 ---------- #
    def quotes(self, symbols: list[str], ttl: float = 18.0) -> dict[str, dict]:
        out: dict[str, dict] = {}
        if not symbols:
            return out
        for i in range(0, len(symbols), self.batch):
            chunk = symbols[i: i + self.batch]
            key = "tx:" + ",".join(chunk)
            hit = self._cache.fresh(key, ttl)
            if hit is not None:
                out.update(hit)
                continue
            try:
                txt = http_get(self.SNAP_URL.format(symbols=",".join("us" + s for s in chunk)),
                               timeout=10, encoding="gbk")
            except SourceError:
                continue
            got: dict[str, dict] = {}
            for line in txt.split(";"):
                if '="' not in line:
                    continue
                head, _, body = line.partition('="')
                sym = head.strip().replace("v_us", "")
                p = body.rstrip('"').split("~")
                if len(p) < 38 or not p[3]:
                    continue

                def f(idx):
                    try:
                        v = p[idx].strip()
                        return float(v) if v not in ("", "-") else None
                    except (ValueError, IndexError):
                        return None

                got[sym] = {
                    "symbol": sym, "source": self.NAME,
                    "name": p[1] or None,
                    "price": f(3), "prev_close": f(4), "open": f(5),
                    "volume": f(6), "chg": f(31), "pct": f(32),
                    "high": f(33), "low": f(34),
                    "amount": f(37),                       # 成交额（美元）
                    "ts_text": p[30] if len(p) > 30 else None,
                    "ts": time.time(),
                }
            self._cache.put(key, got)
            out.update(got)
        return out

    # ---------- 日线（K 线降级用） ---------- #
    def daily(self, symbol: str, n: int = 140) -> dict | None:
        key = f"txd:{symbol}"
        hit = self._cache.fresh(key, 300.0)
        if hit is not None:
            return hit
        try:
            payload = json.loads(http_get(self.FQK_URL.format(sym="us" + symbol, period="day", n=n),
                                          timeout=12))
        except (SourceError, ValueError):
            return None
        node = ((payload or {}).get("data") or {}).get("us" + symbol) or {}
        rows = node.get("qfqday") or node.get("day") or []
        bars = []
        for r in rows:
            if len(r) < 6:
                continue
            try:
                bars.append({"t": r[0], "o": float(r[1]), "c": float(r[2]),
                             "h": float(r[3]), "l": float(r[4]), "v": float(r[5]),
                             "amt": None, "amp": None})
            except ValueError:
                continue
        if not bars:
            return None
        return self._cache.put(key, {"symbol": symbol, "name": None, "klt": 101, "bars": bars})

    def health(self) -> dict:
        return {"name": self.NAME, "role": "主力：实时快照（含成交额）", "cooldown": 0, "fails": 0}


# --------------------------------------------------------------------------- #
# 数据源 0.5：新浪财经（60 分钟历史 K 线 —— 实测最好用的 K 线源）
# --------------------------------------------------------------------------- #
class SinaSource:
    """新浪财经美股分钟级 K 线。

    实测数据（2026-09-24 测定）：
        * 单只 **0.3~0.4 秒**，一次返回 **1023 根**（接口上限）≈ 147 个交易日 ≈ 7 个月；
        * 字段齐全：时间 / 开 / 高 / 低 / 收 / **成交量** / **成交额**；
        * **连续 8 次调用全部成功，没有任何风控迹象**（与东财形成鲜明对比）；
        * 个股与 ETF 都覆盖（AAPL / SPY / QQQ / TQQQ 均实测通过）。

    为什么让它当 K 线主源：
        美股盘前/休市时它返回的就是**完整历史行情**（最后一根＝上一交易日收盘），
        所以「没开盘也能看到 K 线与形态」这个需求天然成立；再加上不限流、历史比东财长得多。

    限制：
        * 返回 JSONP（`var _AAPL_60_1=([...])`），需要剥壳；
        * 时间戳是 **美东时间 ET**（东财是北京时间，两者差 12/13 小时），所以返回里带 `tz` 字段；
        * 只有单只查询接口，没有批量；非公开文档。
    """

    NAME = "新浪财经 (sina)"
    URL = ("https://stock.finance.sina.com.cn/usstock/api/jsonp_v2.php/var%20_{s}_{t}_1=/US_MinKService.getMinK"
           "?symbol={s}&type={t}&___qn=3")
    _TRUST = {"Referer": "https://finance.sina.com.cn/"}

    def __init__(self):
        self._cache = _Cache()

    def klines(self, symbol: str, klt: int = 60, lmt: int = 140) -> dict | None:
        key = f"sina:{symbol}:{klt}:{lmt}"
        hit = self._cache.fresh(key, 25.0)
        if hit is not None:
            return hit
        try:
            raw = http_get(self.URL.format(s=symbol, t=klt), self._TRUST, timeout=12)
        except SourceError:
            old = self._cache.stale(key)
            return old[1] if old else None
        i, j = raw.find("["), raw.rfind("]")
        if i < 0 or j <= i:
            return None
        try:
            rows = json.loads(raw[i:j + 1])
        except ValueError:
            return None
        bars = []
        prev_close = None
        for r in rows:
            try:
                o, h, l, c = float(r["o"]), float(r["h"]), float(r["l"]), float(r["c"])
            except (KeyError, ValueError, TypeError):
                continue
            amp = round((h - l) / prev_close * 100, 3) if prev_close else None
            bars.append({
                "t": (r.get("d") or "")[:16],          # 2026-09-23 16:00
                "o": o, "h": h, "l": l, "c": c,
                "v": float(r.get("v") or 0),
                "amt": float(r.get("a") or 0),
                "amp": amp,
            })
            prev_close = c
        if not bars:
            return None
        out = {"symbol": symbol, "name": None, "klt": klt, "tz": "ET",
               "bars": bars[-lmt:] if lmt else bars}
        return self._cache.put(key, out)

    def health(self) -> dict:
        return {"name": self.NAME,
                "role": "主力：60 分钟 K 线（1023 根 ≈ 147 交易日，含成交额）",
                "cooldown": 0, "fails": 0}


# --------------------------------------------------------------------------- #
# 数据源 3：CNBC 报价（备选 —— 可批量快照，含盘前盘后）
# --------------------------------------------------------------------------- #
class CnbcSource:
    """quote.cnbc.com 报价服务，支持一次请求多只（用 | 分隔），响应 ~0.7s。

    限制：只给快照，没有历史 K 线；非公开文档；批量上限未公开（实测 2 只没问题，
    本项目按每批 20 只切分以控制 URL 长度）。
    """

    NAME = "CNBC (quote.cnbc.com)"
    URL = ("https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol"
           "?symbols={symbols}&requestMethod=itv&noform=1&partnerId=2&fund=1"
           "&exthrs=1&output=json&events=1")

    def __init__(self, batch: int = 20):
        self.batch = batch
        self._cache = _Cache()

    def quotes(self, symbols: list[str]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for i in range(0, len(symbols), self.batch):
            chunk = symbols[i:i + self.batch]
            key = "cnbc:" + ",".join(chunk)
            hit = self._cache.fresh(key, 15.0)
            if hit is not None:
                out.update(hit)
                continue
            try:
                payload = json.loads(http_get(self.URL.format(symbols="|".join(chunk)), timeout=15))
            except Exception:
                continue
            got: dict[str, dict] = {}
            for q in ((payload or {}).get("FormattedQuoteResult") or {}).get("FormattedQuote") or []:
                sym = q.get("symbol")
                if not sym:
                    continue

                def num(v):
                    if v in (None, "", "--"):
                        return None
                    try:
                        return float(str(v).replace(",", "").replace("%", "").replace("+", ""))
                    except ValueError:
                        return None

                got[sym] = {
                    "symbol": sym, "source": self.NAME,
                    "price": num(q.get("last")), "pct": num(q.get("change_pct")),
                    "volume": num(q.get("Volume")), "ts_text": q.get("last_time"),
                    "extended": (q.get("ExtendedMktQuote") or {}).get("last"),
                    "ts": time.time(),
                }
            self._cache.put(key, got)
            out.update(got)
        return out

    def health(self) -> dict:
        return {"name": self.NAME, "role": "备选：批量快照（含盘前盘后）", "cooldown": 0, "fails": 0}


# --------------------------------------------------------------------------- #
# 数据源 0.6：新浪美股报价（hq.sinajs.cn —— 承担「当前这根 60 分钟 bar 现在涨跌多少」）
# --------------------------------------------------------------------------- #
class SinaQuoteSource:
    """hq.sinajs.cn 美股报价，**一次请求可带多只**（逗号分隔），响应约 0.2~0.5 秒。

    为什么非它不可（2026-10-08 实测）：
        * 新浪的**分钟 K 线接口在盘前/盘中不返回「正在走的那根 bar」**——
          盘前 07:30 ET 请求，最后一根仍是上一交易日 16:00，价格也是收盘价；
        * 而本接口的 p[1] 给了**盘前/盘中实时价**（实测 07:30 ET 返回 `Oct 08 07:31AM EDT`
          对应的实时价，秒级跳动），p[21] 给了最近一小时涨幅。

    实测字段（`gb_aapl` = 苹果，0-based，2026-10-08 07:32 ET 取样）：
        0  名称(中文)      1  最新价(盘前/盘中/盘后，实时)   2  涨跌幅%（基准见下⚠️）
        3  行情时间(北京)  4  涨跌额                      5  开盘
        6  最高            7  最低                        8  52周高
        9  52周低          10 成交量(累计)                11 10日均量
        12 市值            13 每股收益                   14 PE
        19 最近1小时成交额 ← 用它判断「这根 60 分钟 bar 走了多少量」
        21 最近1小时价    22 最近1小时涨跌幅%           23 最近1小时涨跌额
        24 行情时间(ET)   25 上一交易日收盘时间(ET)      26 「昨收」（⚠️ 盘前滞后一天）
        30 最近1小时成交额(另一口径，实测与 19 略有出入)

    ⚠️⚠️ **只有 p[1]（实时价）可信**。2026-10-08 实测：盘前请求时 p[26] 给的是
    **前前一日**的收盘（MU 报 1045.56 = 10-06 收盘，而 10-07 真实收盘是 1087.835），
    p[2]/p[4] 基于这个错误基准算出来（MU 显示 +4.06%，真实仅 +0.015%）。腾讯快照
    的 p[4] 有同样的毛病。⇒ **昨收一律以 60 分钟 K 线尾部的收盘价为准**
    （`server.Monitor._fix_rt_baseline` / `current_bar.resolve_current_bar` 都这么做）。

    限制：非公开文档；字段位置靠约定；**ETF 也覆盖**（SPY/QQQ/IVV 实测通过）。
    """

    NAME = "新浪报价 (hq.sinajs.cn)"
    URL = "https://hq.sinajs.cn/list={list}"
    _TRUST = {"Referer": "https://finance.sina.com.cn/"}
    BATCH = 60

    def __init__(self):
        self._cache = _Cache()

    @staticmethod
    def _f(p, i):
        try:
            v = p[i].strip()
            return float(v) if v not in ("", "-", "0.00" if i in (22, 23) else "\x00") else None
        except (ValueError, IndexError):
            return None

    def quotes(self, symbols: list[str], ttl: float = 15.0) -> dict[str, dict]:
        out: dict[str, dict] = {}
        if not symbols:
            return out
        for i in range(0, len(symbols), self.BATCH):
            chunk = symbols[i: i + self.BATCH]
            key = "sinahq:" + ",".join(chunk)
            hit = self._cache.fresh(key, ttl)
            if hit is not None:
                out.update(hit)
                continue
            url = self.URL.format(list=",".join("gb_" + s.lower() for s in chunk))
            try:
                raw = http_get(url, self._TRUST, timeout=10, encoding="gbk")
            except SourceError:
                continue
            got: dict[str, dict] = {}
            for line in raw.splitlines():
                if "hq_str_gb_" not in line or '="' not in line:
                    continue
                head, _, body = line.partition('="')
                sym = head.split("hq_str_gb_")[-1].strip().upper()
                body = body.rstrip('";').rstrip('"')
                if not body:
                    continue
                p = body.split(",")
                if len(p) < 27:
                    continue
                got[sym] = {
                    "symbol": sym, "source": self.NAME,
                    "name": p[0] or None,
                    "price": self._f(p, 1),                # 盘前/盘中/盘后实时价 ← 唯一可信字段
                    # ⚠️ p[2]/p[4]/p[26] 在**盘前时段基准滞后一天**（见下方注释），
                    # 一律加 `raw_` 前缀标明「原样透传、不可直接用于涨跌幅」。
                    "raw_pct": self._f(p, 2),              # 相对（错误的）昨收，勿用
                    "raw_chg": self._f(p, 4),
                    "raw_prev_close": self._f(p, 26),
                    "open": self._f(p, 5), "high": self._f(p, 6), "low": self._f(p, 7),
                    "volume": self._f(p, 10),
                    "amount_1h": self._f(p, 30) or self._f(p, 19),   # 最近 1 小时成交额
                    "px_1h": self._f(p, 21),               # 最近 1 小时价
                    "pct_1h": self._f(p, 22),              # 最近 1 小时涨跌幅（同源，参考）
                    "ts_text": p[24] if len(p) > 24 else None,       # 行情时间（ET）
                    "prev_ts_text": p[25] if len(p) > 25 else None,  # 该源自称的上一收盘时间
                    "ts": time.time(),
                }
            self._cache.put(key, got)
            out.update(got)
        return out

    def health(self) -> dict:
        return {"name": self.NAME,
                "role": "主力：盘前/盘中实时价（供「当前 60 分钟 bar」使用，批量 60 只/次）",
                "cooldown": 0, "fails": 0}


# --------------------------------------------------------------------------- #
# 门面：给 server.py 用的统一入口
# --------------------------------------------------------------------------- #
class MarketData:
    """把各个源组合成一个门面对象，对外只暴露三件事：榜单、K线、实时价。

    分工（这是本项目的核心设计）：
        榜单「谁在前 30」  -> 东财 clist（唯一能一次给出全市场成交额排序的免费源，低频）
        价格「现在多少钱」 -> 腾讯 qt.gtimg.cn（0.1 秒批量 200 只，可高频）
        K 线「60 分钟形态」-> 东财 push2his（klt=60），取不到时降级腾讯日线

    降级链：
        东财榜单不可用 -> 本地候选池 + 腾讯实时成交额，在本地重排（会标注 degraded）
        东财 K 线不可用 -> 腾讯日线（会标注周期降级）
        腾讯快照不可用 -> 保留东财榜单自带的价格字段（会显示数据时间）
    """

    def __init__(self, cfg: dict):
        bcfg = cfg.get("board", {})
        qcfg = cfg.get("quote", {})
        self.em = EastmoneySource(board_min_interval=bcfg.get("min_interval_sec", 60))
        self.tx = TencentSource(batch=qcfg.get("batch", 200))
        self.sina = SinaSource()
        self.sinaq = SinaQuoteSource()
        self.nq = NasdaqSource()
        self.cnbc = CnbcSource()
        self.board_ttl = bcfg.get("refresh_sec", 60)
        self.pool_size = bcfg.get("pool_size", 200)
        self.quote_ttl = qcfg.get("refresh_sec", 18)
        self.kline_cfg = cfg.get("kline", {})
        self._cache_dir = os.path.join(BASE_DIR, "cache")
        self._universe = self._load_universe()

    # ---------------- 候选池（降级用） ---------------- #
    def _load_universe(self) -> list[dict]:
        path = os.path.join(self._cache_dir, "universe.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass
        return [{"symbol": s, "name": s, "market": 105} for s in BUILTIN_UNIVERSE]

    def _save_universe(self, pool: list[dict]) -> None:
        if not pool:
            return
        self._universe = [{"symbol": it["symbol"], "name": it.get("name"),
                           "market": it.get("market")} for it in pool if it.get("symbol")]
        try:
            os.makedirs(self._cache_dir, exist_ok=True)
            with open(os.path.join(self._cache_dir, "universe.json"), "w", encoding="utf-8") as f:
                json.dump(self._universe, f, ensure_ascii=False)
        except Exception:
            pass

    # ---------------- 榜单 ---------------- #
    def board(self, top_n: int = 30) -> dict:
        res = self.em.board(top_n, ttl=self.board_ttl, pool_size=self.pool_size)
        items = res.get("items") or []
        if res.get("ok") and res.get("pool"):
            self._save_universe(res["pool"])            # 记住这次的真实榜单，下次降级有底

        if not items:
            items = self._local_board(top_n)
            if items:
                res["items"] = items
                res["ok"] = True
                res["stale"] = False                       # 数据是新鲜的，只是排名依据换成了本地重算
                res.pop("stale_age", None)
                res["degraded"] = True
                res["source"] = "本地重建：候选池(" + str(len(self._universe)) + ") × " + self.tx.NAME
                tag = " ｜ 降级中：东财榜单不可用，已用本地候选池按腾讯实时成交额重排"
                res["note"] = (res.get("note") or "") + tag

        self.enrich(items)
        res["items"] = items
        return res

    def _local_board(self, top_n: int) -> list[dict]:
        """用本地候选池 + 腾讯快照在本地算成交额榜。"""
        uni = self._universe or []
        if not uni:
            return []
        snap = self.tx.quotes([u["symbol"] for u in uni], ttl=self.quote_ttl)
        rows: list[dict] = []
        for u in uni:
            q = snap.get(u["symbol"])
            if not q or q.get("amount") is None:
                continue
            rows.append({
                "symbol": u["symbol"], "market": u.get("market", 105),
                "market_name": EastmoneySource.MARKET_NAME.get(u.get("market"), "?"),
                "name": u.get("name") or q.get("name") or u["symbol"],
                "price": q.get("price"), "pct": q.get("pct"), "chg": q.get("chg"),
                "volume": q.get("volume"), "amount": q.get("amount"),
                "high": q.get("high"), "low": q.get("low"), "open": q.get("open"),
                "prev_close": q.get("prev_close"),
                "rt": {"source": q["source"], "ts": q.get("ts_text")},
            })
        rows.sort(key=lambda x: x.get("amount") or 0, reverse=True)
        out = rows[:top_n]
        for i, it in enumerate(out, 1):
            it["rank"] = i
        return out

    # ---------------- 实时价格回填 ---------------- #
    def enrich(self, items: list[dict]) -> list[dict]:
        """用腾讯快照把榜单里的价格类字段刷新成实时的。

        榜单 60 秒才取一次（受东财风控约束），但价格必须是最新的 —— 这一层不可或缺。

        ⚠️ 昨收/涨跌幅**不采信腾讯快照**（2026-10-08 实测：盘前 p[4] 给的是前前一日
        收盘，MU 报 1045.56，真实为 1087.835 ⇒ 涨跌幅从 +0.02% 错成 +4.06%）。
        这里先从 60 分钟 K 线拿正确的「最后一根收盘」（= 真实昨收）来重算 pct/chg；
        K 线取不到时才退回腾讯原值（并标注 `pct_src=tx_raw` 以示不可比）。

        ⭐ 2026-10-08 老大定调「**只看盘中**」：
          * 盘中（09:30–16:00 ET）：正常用实时价刷新，pct = 现价 vs 上一根收盘。
          * 盘前/盘后/休市：**不使用盘前/盘后报价当现价**，价格一律钉在「最近一次
            盘中收盘」（K 线最后一根），此时 pct 恒为 0（因为现价就是昨收）。
            这样页面不会出现盘前跳动的数字，也不会有「盘前涨跌」这种假信号。
        """
        if not items:
            return items
        syms = [it["symbol"] for it in items]
        snap = self.tx.quotes(syms, ttl=self.quote_ttl)
        bases = self._last_closes(syms)
        intraday = _in_regular_session()
        for it in items:
            q = snap.get(it["symbol"])
            base = bases.get(it["symbol"])
            # --- 盘前/盘后/休市：价格钉在最近一次盘中收盘 ---
            if not intraday:
                if base:
                    it["price"] = base["c"]
                    it["prev_close"] = base["c"]
                    it["prev_close_t"] = base["t"]
                    it["chg"] = 0.0
                    it["pct"] = 0.0
                    it["pct_src"] = "last_intraday_close"
                    it["rt"] = {"source": "最近盘中收盘", "ts": base.get("t")}
                continue
            if not q:
                continue
            for k in ("price", "volume", "amount", "high", "low", "open"):
                if q.get(k) is not None:
                    it[k] = q[k]
            it["rt"] = {"source": q["source"], "ts": q.get("ts_text")}
            if base and q.get("price") is not None:
                it["prev_close"] = base["c"]
                it["prev_close_t"] = base["t"]
                it["chg"] = round(q["price"] - base["c"], 4)
                it["pct"] = round((q["price"] / base["c"] - 1) * 100, 3)
                it["pct_src"] = "kline"
            else:
                for k in ("pct", "chg", "prev_close"):
                    if q.get(k) is not None:
                        it[k] = q[k]
                it["pct_src"] = "tx_raw"
        return items

    def _last_closes(self, symbols: list[str]) -> dict[str, dict]:
        """批量取每只「60 分钟 K 线最后一根」的 {t, c} —— 用作**可信的昨收基准**。

        60 分钟 K 线尾部的收盘价经交叉验证就是上一交易日收盘（见 README 的踩坑记录），
        所以拿它当涨跌幅分母，与自选框的 pct 同口径。

        实现走 `klines60()`（内部已有 4 并发 + 25 秒缓存），比逐只串行请求快得多；
        只保留最后一根，省内存。
        """
        out: dict[str, dict] = {}
        try:
            raw = self.klines60([{"symbol": s, "market": 105} for s in symbols])
        except Exception:
            return out
        for sym, kl in (raw or {}).items():
            bars = (kl or {}).get("bars") or []
            if bars:
                out[sym] = {"t": bars[-1].get("t"), "c": bars[-1].get("c")}
        return out

    # ---------------- K 线 ---------------- #
    def kline_one(self, symbol: str, market: int | None = None,
                  klt: int | None = None, lmt: int | None = None) -> dict | None:
        """单只 K 线降级链：**新浪（主）→ 东财（备）**。

        新浪返回 1023 根、不限流、含成交额，而且盘前/休市时给的就是历史行情；
        东财只在标的时间戳是北京时间，作为备份。
        """
        klt = klt or self.kline_cfg.get("period", 60)
        lmt = lmt or self.kline_cfg.get("bars", 140)
        got = self.sina.klines(symbol, klt, lmt)
        if got and got.get("bars"):
            return got
        if market:
            got = self.em.klines(symbol, market, klt, lmt)
            if got and got.get("bars"):
                got["tz"] = "CST"                      # 东财时间戳是北京时间
                return got
        return None

    def klines60(self, items: list[dict]) -> dict[str, dict]:
        """批量取 60 分钟 K 线（线程池并发 + 把请求分摊开）。"""
        klt = self.kline_cfg.get("period", 60)
        lmt = self.kline_cfg.get("bars", 140)
        conc = self.kline_cfg.get("concurrency", 4)
        gap = self.kline_cfg.get("gap_ms", 80)
        out: dict[str, dict] = {}
        if not items:
            return out
        workers = max(1, min(conc, len(items)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {}
            for it in items:
                if not it.get("symbol"):
                    continue
                futs[pool.submit(self.kline_one, it["symbol"], it.get("market"),
                                 klt, lmt)] = it
                if gap:
                    time.sleep(gap / 1000.0)
            for fut in futs:
                it = futs[fut]
                try:
                    res = fut.result()
                except Exception:
                    res = None
                if res:
                    out[it["symbol"]] = res
        return out

    # ---------------- 实时（单只详情） ---------------- #
    def realtime(self, symbol: str, name: str = "") -> dict | None:
        got = self.tx.quotes([symbol], ttl=0.0).get(symbol)
        if got and got.get("price") is not None:
            got["extended"] = None
            return got
        klass = self.nq.guess_class(name, symbol)
        got = self.nq.realtime(symbol, klass)
        if got:
            return got
        return self.cnbc.quotes([symbol]).get(symbol)

    def quotes_now(self, symbols: list[str]) -> dict[str, dict]:
        """盘前/盘中实时价（批量）—— 专门用来算「当前这根 60 分钟 bar」。

        与 tx.quotes 的区别：腾讯在**盘前返回的是上一交易日收盘快照**（实测
        07:32 ET 给的是 10-07 16:00:01 的数据），所以盘前必须换新浪报价。
        这里两个源都取，新浪优先（它盘前/盘中/盘后都有值），腾讯兜底。
        """
        out: dict[str, dict] = {}
        if not symbols:
            return out
        out.update(self.sinaq.quotes(symbols, ttl=self.quote_ttl))
        miss = [s for s in symbols if s not in out]
        if miss:
            out.update(self.tx.quotes(miss, ttl=self.quote_ttl))
        return out

    def health(self) -> dict:
        return {"sina": self.sina.health(), "sina_quote": self.sinaq.health(),
                "tencent": self.tx.health(),
                "eastmoney": self.em.health(), "nasdaq": self.nq.health(),
                "cnbc": self.cnbc.health(), "universe": len(self._universe)}
