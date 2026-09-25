# -*- coding: utf-8 -*-
"""
us_monitor / universe.py
========================
内置「候选池」—— 降级兜底用。

为什么需要它：
    全市场成交额排序只能靠东财的服务端排序接口，而那个接口有风控。
    一旦它被限制，工具不应该变成空白页，而应该用「本地候选池 + 腾讯实时快照」
    在本地把榜单重新算出来。

这个池子的正确用法：
    东财每次成功抓取时，都会把**成交额前 200 名**覆盖写入 cache/universe.json，
    所以正常情况下用的其实是「你上一次真实榜样的前 200 名」，准确度很高。
    只有当程序第一次运行、或缓存被删掉时，才会退回这份内置清单。

维护建议：这份清单覆盖了标普 500 高权重成分股 + 中概龙头 + 主流 ETF，
成交额前 30 名基本跑不出这个范围。想更保险就往里面加代码。
"""

BUILTIN_UNIVERSE = [
    # ---- 科技 / 半导体 ----
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "AVGO", "TSM",
    "ORCL", "CRM", "ADBE", "AMD", "INTC", "QCOM", "TXN", "MU", "AMAT", "LRCX",
    "KLAC", "MRVL", "ARM", "SMCI", "NOW", "UBER", "ABNB", "SHOP", "PYPL", "COIN",
    "SNOW", "DDOG", "NET", "CRWD", "ZS", "PANW", "FTNT", "ADSK", "WDAY", "TEAM",
    "MDB", "IBM", "CSCO", "DELL", "HPQ", "HPE", "ON", "NXPI", "ADI", "MCHP",
    "ANET", "APH", "MSI", "GLW", "WDC", "STX", "TER", "SWKS", "QRVO", "SNPS",
    "CDNS", "INTU", "ANSS", "PTC", "ROP", "TTD", "SPOT", "PINS", "SNAP", "RBLX",
    "U", "DASH", "LYFT", "ZM", "DOCU", "TWLO", "OKTA", "PLTR", "AI", "PATH",
    # ---- 金融 ----
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "AXP", "V",
    "MA", "BRK.B", "SPGI", "CB", "MMC", "AON", "ICE", "CME", "NDAQ", "COF",
    "USB", "PNC", "TFC", "BK", "STT", "MET", "PRU", "AIG", "ALL", "PGR",
    "TRV", "AFL", "HOOD", "SOFI",
    # ---- 消费 ----
    "WMT", "COST", "HD", "LOW", "TGT", "NKE", "SBUX", "MCD", "DIS", "NFLX",
    "CMCSA", "CHTR", "TMUS", "VZ", "T", "KO", "PEP", "PG", "PM", "MO",
    "MDLZ", "CL", "KMB", "GIS", "K", "HSY", "STZ", "KHC", "SYY", "KR",
    "LULU", "ROST", "TJX", "DG", "DLTR", "BBY", "EBAY", "ETSY", "CVNA", "YUM",
    # ---- 医药 ----
    "UNH", "JNJ", "LLY", "PFE", "MRK", "ABBV", "TMO", "ABT", "DHR", "BMY",
    "AMGN", "GILD", "CVS", "CI", "ELV", "HUM", "MDT", "SYK", "BSX", "ZBH",
    "ISRG", "VRTX", "REGN", "MRNA", "BIIB", "ILMN", "DXCM", "IDXX", "A", "EW",
    "HCA", "MCK", "COR", "CAH", "BDX", "BAX",
    # ---- 工业 / 能源 / 材料 ----
    "CAT", "BA", "GE", "HON", "UPS", "RTX", "DE", "LMT", "NOC", "GD",
    "MMM", "EMR", "ETN", "PH", "CMI", "PCAR", "CSX", "UNP", "NSC", "FDX",
    "XOM", "CVX", "COP", "SLB", "OXY", "PSX", "VLO", "MPC", "KMI", "WMB",
    "LIN", "APD", "SHW", "FCX", "NEM", "NUE", "DOW", "DD",
    # ---- 公用 / 地产 / 电信 ----
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "PLD", "AMT", "EQIX",
    "CCI", "SPG", "O", "PSA",
    # ---- 中概 ----
    "BABA", "PDD", "JD", "NTES", "BIDU", "NIO", "XPEV", "LI", "TME", "IQ",
    "BILI", "BEKE", "TCOM", "YUMC", "EDU", "TAL", "VIPS", "MNSO", "FUTU", "TIGR",
    # ---- 指数 / 宽基 ETF ----
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV", "VEA", "VWO", "EEM",
    "IEMG", "FXI", "MCHI", "KWEB", "ASHR", "VGK", "EWJ", "EWZ", "EWY", "INDA",
    # ---- 行业 / 主题 ETF ----
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE",
    "SMH", "SOXX", "XBI", "IBB", "KRE", "XOP", "XME", "ITB", "XRT", "JETS",
    # ---- 债券 / 商品 / 波动率 ETF ----
    "TLT", "IEF", "SHY", "LQD", "HYG", "AGG", "BND", "TIP", "MUB", "EMB",
    "GLD", "IAU", "SLV", "USO", "UNG", "DBC", "GDX", "GDXJ", "UUP", "FXE",
    # ---- 杠杆 / 反向（日内波动大，经常进成交额榜） ----
    "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXU", "UPRO", "TNA", "TZA", "UVXY",
    "VXX", "ARKK", "LABU", "LABD", "FNGU", "YINN", "YANG",
]
