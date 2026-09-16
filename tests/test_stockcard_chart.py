"""T0-b 단위 테스트 — 파일명 규칙 · 짧은 이력 · 실패 격리 · 폰트 폴백.

네트워크를 타지 않는다. fetch_ohlc 를 합성 일봉으로 갈아끼운다.
"""

import pandas as pd
import pytest

import stockcard_chart as chart


def fake_ohlc(n=60, start="2026-06-23"):
    idx = pd.bdate_range(start, periods=n)
    base = 7000
    return pd.DataFrame({
        "Open":   [base + i * 10 for i in range(n)],
        "High":   [base + i * 10 + 200 for i in range(n)],
        "Low":    [base + i * 10 - 200 for i in range(n)],
        "Close":  [base + i * 10 + (50 if i % 2 else -50) for i in range(n)],
        "Volume": [1_000_000 + i * 1000 for i in range(n)],
    }, index=idx)


@pytest.fixture
def no_network(monkeypatch):
    monkeypatch.setattr(chart, "fetch_ohlc",
                        lambda ticker, date, bars: fake_ohlc(min(bars, 60)))


def test_파일명_규칙(tmp_path):
    p = chart.chart_path("115440", "20260916", "pm", tmp_path)
    assert p.name == "115440_20260916_pm.png"


def test_파일명은_종목코드를_6자리로_채운다(tmp_path):
    assert chart.chart_path("5930", "20260916", "am", tmp_path).name \
        == "005930_20260916_am.png"


def test_차트가_생성된다(tmp_path, no_network):
    p = chart.render("115440", "20260916", "pm", charts_dir=tmp_path)
    assert p is not None and p.exists() and p.stat().st_size > 5000


def test_60봉_미만도_생성된다(tmp_path, monkeypatch):
    """상장 직후 종목. MA20 이 안 그려질 뿐 차트는 나와야 한다."""
    monkeypatch.setattr(chart, "fetch_ohlc", lambda t, d, b: fake_ohlc(8))
    p = chart.render("123456", "20260916", "pm", charts_dir=tmp_path)
    assert p is not None and p.exists()


def test_일봉이_없으면_파일을_만들지_않는다(tmp_path, monkeypatch):
    monkeypatch.setattr(chart, "fetch_ohlc", lambda t, d, b: None)
    assert chart.render("999999", "20260916", "pm", charts_dir=tmp_path) is None
    assert not list(tmp_path.glob("*.png"))


def test_한_종목이_실패해도_나머지는_그린다(tmp_path, monkeypatch):
    """차트 실패가 발송이나 수급 적재를 막으면 안 된다."""
    def flaky(ticker, date, bars):
        if ticker == "000000":
            raise RuntimeError("일부러 낸 오류")
        return fake_ohlc(30)

    monkeypatch.setattr(chart, "fetch_ohlc", flaky)
    s = chart.render_all(["115440", "000000", "000660"],
                         "20260916", "pm", charts_dir=tmp_path)
    assert s["ok"] == 2 and s["failed"] == 1
    assert len(list(tmp_path.glob("*.png"))) == 2


def test_한글_폰트가_없으면_제목에서_종목명을_뺀다(tmp_path, monkeypatch, no_network):
    """ubuntu 러너에는 한글 폰트가 없다. 로컬(macOS)에서는 드러나지 않는 경로다."""
    monkeypatch.setattr(chart, "_FONT_CHECKED", False)
    monkeypatch.setattr(chart, "_FONT_NAME", None)
    monkeypatch.setattr(chart.fm.fontManager, "ttflist", [])
    assert chart.korean_font() is None
    # 폰트가 없어도 렌더 자체는 성공해야 한다
    p = chart.render("115440", "20260916", "pm", name="우리넷", charts_dir=tmp_path)
    assert p is not None and p.exists()
