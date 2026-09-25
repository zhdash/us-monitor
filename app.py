# -*- coding: utf-8 -*-
"""
us_monitor / app.py
===================
启动器：起服务 + 自动打开浏览器。

直接运行：
    python app.py                 # 用 config.json 里的端口（默认 8021）
    python app.py --port 8099     # 临时换端口
    python app.py --no-browser    # 不自动开浏览器（后台/远程跑时用）
"""
from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

import server  # noqa: E402


def _open_later(port: int) -> None:
    """等服务真正起来再开浏览器，避免打开一个连接被拒的空白页。"""
    import urllib.request
    url = "http://127.0.0.1:%d" % port
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for _ in range(30):
        time.sleep(0.4)
        try:
            opener.open(url + "/api/config", timeout=2).read()
            break
        except Exception:
            continue
    try:
        webbrowser.open(url)
    except Exception:
        pass


def main() -> None:
    args = sys.argv[1:]
    port = None
    if "--port" in args:
        try:
            port = int(args[args.index("--port") + 1])
        except (IndexError, ValueError):
            print("--port 需要跟一个端口号"); return
    cfg = server.load_config()
    port = port or int(os.environ.get("US_MONITOR_PORT") or cfg.get("port", 8021))

    if "--no-browser" not in args:
        threading.Thread(target=_open_later, args=(port,), daemon=True).start()
    server.main(port)


if __name__ == "__main__":
    main()
