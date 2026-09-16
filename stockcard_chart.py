"""T0-b — 종목 차트 PNG (60일 봉 + 20일선 + 거래량).

PNG 는 git 에 커밋하지 않는다. OHLC 에서 언제든 다시 그릴 수 있는 산출물이고,
git 은 지운 파일도 히스토리에 영구 보관한다(8/24 원칙). Actions 에서는
upload-artifact(retention 30일)로 남기고, 로컬에서는 재생성한다.

차트 실패가 발송이나 수급 적재를 막아서는 안 된다 — 호출부에서 종목 단위로
예외를 삼키고, 실패한 종목은 로그에만 남긴다.
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # 헤드리스 러너용. pyplot import 전에 지정해야 한다.

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager as fm  # noqa: E402

import alpharadar as ar  # noqa: E402
import stockcard_common as sc  # noqa: E402

# 한국 관례: 상승 빨강, 하락 파랑
UP, DOWN, MA_COLOR = "#d24545", "#3d6fb4", "#e0a030"

_FONT_CHECKED = False
_FONT_NAME = None

# ubuntu 러너에는 한글 폰트가 없어 종목명이 □□□ 로 나온다. 로컬(macOS)에서는
# 멀쩡해서 드러나지 않으므로, 폰트가 없으면 제목에서 이름을 빼는 폴백을 둔다.
_CANDIDATES = ("NanumGothic", "Nanum Gothic", "NanumBarunGothic",
               "Malgun Gothic", "AppleGothic", "Apple SD Gothic Neo",
               "Noto Sans CJK KR", "Noto Sans KR")


def korean_font() -> str | None:
    """설치된 한글 폰트 이름. 없으면 None — 호출부가 제목을 코드만으로 만든다."""
    global _FONT_CHECKED, _FONT_NAME
    if _FONT_CHECKED:
        return _FONT_NAME
    have = {f.name for f in fm.fontManager.ttflist}
    _FONT_NAME = next((n for n in _CANDIDATES if n in have), None)
    if _FONT_NAME:
        plt.rcParams["font.family"] = _FONT_NAME
    else:
        sc.logger().warning(
            "한글 폰트 없음 — 차트 제목을 종목코드만으로 그린다 "
            "(Actions: apt-get install fonts-nanum)")
    plt.rcParams["axes.unicode_minus"] = False
    _FONT_CHECKED = True
    return _FONT_NAME


def chart_path(ticker: str, date: str, run_type: str, charts_dir) -> Path:
    return Path(charts_dir) / f"{str(ticker).zfill(6)}_{date}_{run_type}.png"


def fetch_ohlc(ticker: str, date: str, bars: int):
    """date 까지의 최근 bars 개 일봉. 상장 직후 종목은 있는 만큼만 돌려준다."""
    import FinanceDataReader as fdr

    end = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    # 60거래일을 확보하려면 달력일로 넉넉히 잡아야 한다(휴장·연휴).
    start_dt = datetime.strptime(date, "%Y%m%d") - timedelta(days=bars * 2 + 40)
    df = fdr.DataReader(ticker, start_dt.strftime("%Y-%m-%d"), end)
    if df is None or not len(df):
        return None
    return df.tail(bars)


def render(ticker: str, date: str, run_type: str, name: str = "",
           charts_dir="data/charts", bars: int = 60, ma: int = 20) -> Path | None:
    """PNG 한 장. 데이터가 없으면 None 을 돌려주고 파일을 만들지 않는다."""
    ticker = str(ticker).zfill(6)
    df = fetch_ohlc(ticker, date, bars)
    if df is None or len(df) < 2:
        sc.logger().warning(f"차트 생략 ({ticker}) — 일봉 없음")
        return None

    font = korean_font()
    title = f"{ticker} {name}".strip() if (name and font) else ticker

    out = chart_path(ticker, date, run_type, charts_dir)
    out.parent.mkdir(parents=True, exist_ok=True)

    fig, (ax, axv) = plt.subplots(
        2, 1, figsize=(10, 6), sharex=True,
        gridspec_kw={"height_ratios": [3, 1], "hspace": 0.05})

    x = range(len(df))
    o, h, lo, c = (df["Open"].values, df["High"].values,
                   df["Low"].values, df["Close"].values)
    colors = [UP if c[i] >= o[i] else DOWN for i in x]

    # 봉: 고저는 얇은 선, 시종가는 몸통. bar 두 번이 candlestick 라이브러리보다 가볍다.
    ax.vlines(x, lo, h, color=colors, linewidth=0.8)
    ax.bar(x, (c - o), bottom=o, color=colors, width=0.6, linewidth=0)

    if len(df) >= ma:
        ma_s = df["Close"].rolling(ma).mean()
        ax.plot(x, ma_s.values, color=MA_COLOR, linewidth=1.2, label=f"MA{ma}")
        ax.legend(loc="upper left", fontsize=8, frameon=False)

    ax.set_title(f"{title}  ·  {date} {run_type}", fontsize=11, loc="left")
    ax.grid(alpha=0.2, linewidth=0.5)
    ax.set_ylabel("주가")

    axv.bar(x, df["Volume"].values, color=colors, width=0.6, linewidth=0)
    axv.grid(alpha=0.2, linewidth=0.5)
    axv.set_ylabel("거래량")

    # 날짜 눈금은 10개만 — 60개를 다 찍으면 읽을 수 없다.
    step = max(1, len(df) // 10)
    ticks = list(x)[::step]
    axv.set_xticks(ticks)
    axv.set_xticklabels([df.index[i].strftime("%m/%d") for i in ticks],
                        fontsize=8, rotation=0)

    fig.savefig(out, dpi=110, bbox_inches="tight")
    plt.close(fig)
    return out


def render_all(tickers, date: str, run_type: str, names=None,
               charts_dir="data/charts", bars=60, ma=20) -> dict:
    """종목 단위로 실패를 삼킨다 — 한 장이 깨져도 나머지는 그린다."""
    log = sc.logger()
    names = names or {}
    stats = {"ok": 0, "failed": 0, "paths": []}
    for t in tickers:
        try:
            p = render(t, date, run_type, names.get(str(t).zfill(6), ""),
                       charts_dir, bars, ma)
        except Exception as e:
            log.warning(f"차트 실패 ({t}): {type(e).__name__}: {e}")
            stats["failed"] += 1
            continue
        if p is None:
            stats["failed"] += 1
        else:
            stats["ok"] += 1
            stats["paths"].append(p)
    log.info(f"차트: {stats['ok']}장 생성 (실패 {stats['failed']})")
    return stats


def ticker_names(tickers, scan_date: str) -> dict:
    """scan_results 에 있는 종목명. watchlist 전용 종목은 비어 있을 수 있다."""
    if not tickers:
        return {}
    try:
        with ar._conn() as con:
            q = ",".join("?" * len(tickers))
            rows = con.execute(
                f"SELECT ticker, name FROM scan_results "
                f"WHERE scan_date = ? AND ticker IN ({q})",
                [scan_date, *tickers]).fetchall()
    except Exception:
        return {}
    return {str(t).zfill(6): n for t, n in rows if n}


def main(argv=None):
    p = argparse.ArgumentParser(description="stockcard T0-b — 차트 PNG")
    p.add_argument("--ticker", action="append")
    p.add_argument("--date", default=None)
    p.add_argument("--run-type", choices=["am", "pm"], default=None)
    args = p.parse_args(argv)

    scfg = sc.sc_config()
    ccfg = scfg.get("chart", {}) or {}
    date = (args.date or ar.today_kst()).replace("-", "")
    run_type = args.run_type or ar.run_type_kst()

    if args.ticker:
        tickers = [str(t).zfill(6) for t in args.ticker]
    else:
        tickers, origin = sc.resolve_targets(
            date, run_type, scfg.get("watchlist_path"))
        sc.logger().info(
            f"대상 {origin['total']}종목 "
            f"(스캔 {origin['scan']} + watchlist 전용 {origin['watchlist_only']})")

    if not tickers:
        sc.logger().warning("대상 종목이 없다 — 차트 생략")
        return 0

    render_all(tickers, date, run_type, ticker_names(tickers, date),
               scfg.get("charts_dir", "data/charts"),
               ccfg.get("bars", 60), ccfg.get("ma", 20))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
