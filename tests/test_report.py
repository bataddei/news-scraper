"""Unit tests for the daily report module.

Focuses on the pure formatting layer. DB fetch functions are thin SQL
shims exercised via the live CLI.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest

from news_archive.monitoring.gaps import Gap
from news_archive.monitoring.report import (
    DiskUsage,
    ReportData,
    SourceRow,
    _bytes_pretty,
    _format_gaps_section,
    _format_per_source_table,
    format_report,
    send_telegram,
)

GEN_AT = datetime(2026, 4, 22, 8, 0, 0, tzinfo=UTC)


def _sr(slug: str, ins: int = 0, dup: int = 0, seen: int = 0, fail: int = 0) -> SourceRow:
    return SourceRow(
        slug=slug,
        success_runs=1,
        partial_runs=0,
        failed_runs=fail,
        articles_seen=seen,
        articles_inserted=ins,
        articles_duplicate=dup,
    )


class TestBytesPretty:
    def test_bytes(self) -> None:
        assert _bytes_pretty(500) == "500 B"

    def test_kilobytes(self) -> None:
        assert "KB" in _bytes_pretty(2048)

    def test_megabytes(self) -> None:
        assert "MB" in _bytes_pretty(5 * 1024 * 1024)

    def test_gigabytes(self) -> None:
        assert "GB" in _bytes_pretty(3 * 1024**3)


class TestFormatTable:
    def test_empty(self) -> None:
        assert _format_per_source_table([]) == "(no sources)"

    def test_columns_aligned(self) -> None:
        rows = [
            _sr("fed_speeches", ins=12, dup=4, seen=16),
            _sr("sec_edgar_mag7", ins=45, dup=135, seen=180, fail=1),
        ]
        out = _format_per_source_table(rows)
        lines = out.splitlines()
        assert "source" in lines[0]
        assert "ins" in lines[0]
        assert "fed_speeches" in lines[2]
        assert "sec_edgar_mag7" in lines[3]

    def test_long_slug_truncated(self) -> None:
        rows = [_sr("x" * 50)]
        out = _format_per_source_table(rows)
        assert "…" in out  # truncation marker


class TestFormatGapsSection:
    def test_empty(self) -> None:
        assert _format_gaps_section([]) == "✅ No gaps."

    def test_never_run(self) -> None:
        gap = Gap(
            source_slug="treasury_press",
            source_id=4,
            kind="never_run",
            last_success_at=None,
            seconds_since_last=None,
            max_gap_seconds=9000,
        )
        out = _format_gaps_section([gap])
        assert "treasury_press" in out
        assert "never produced" in out

    def test_overdue(self) -> None:
        gap = Gap(
            source_slug="gdelt_gkg",
            source_id=6,
            kind="overdue",
            last_success_at=datetime(2026, 4, 22, 0, 0, 0, tzinfo=UTC),
            seconds_since_last=8 * 3600,
            max_gap_seconds=9000,
        )
        out = _format_gaps_section([gap])
        assert "gdelt_gkg" in out
        assert "480m ago" in out

    def test_html_escapes_slug(self) -> None:
        # Safety check — if a source slug ever contained HTML metachars we mustn't
        # inject raw HTML into Telegram's parser.
        gap = Gap(
            source_slug="<script>",
            source_id=99,
            kind="never_run",
            last_success_at=None,
            seconds_since_last=None,
            max_gap_seconds=0,
        )
        out = _format_gaps_section([gap])
        assert "<script>" not in out
        assert "&lt;script&gt;" in out


class TestFormatReport:
    def _data(self, gaps: list[Gap] | None = None) -> ReportData:
        return ReportData(
            generated_at=GEN_AT,
            per_source=[
                _sr("fed_speeches", ins=12, dup=4, seen=16),
                _sr("gdelt_gkg", ins=1433, dup=0, seen=1915),
            ],
            total_articles=12345,
            db_size_pretty="42 MB",
            disk=DiskUsage(total_bytes=10 * 1024**3, used_bytes=3 * 1024**3, free_bytes=7 * 1024**3),
            gaps=gaps or [],
        )

    def test_includes_all_sections(self) -> None:
        out = format_report(self._data())
        assert "Daily Report" in out
        assert "2026-04-22 08:00 UTC" in out
        assert "fed_speeches" in out
        assert "gdelt_gkg" in out
        assert "12,345" in out  # thousands-separator
        assert "42 MB" in out
        assert "✅ No gaps." in out

    def test_inserted_total_in_header(self) -> None:
        out = format_report(self._data())
        # 12 + 1433 = 1445
        assert "1,445 inserted" in out

    def test_disk_percent_rendered(self) -> None:
        out = format_report(self._data())
        assert "30% used" in out  # 3/10 = 30%

    def test_gaps_rendered_when_present(self) -> None:
        gap = Gap(
            source_slug="treasury_press",
            source_id=4,
            kind="never_run",
            last_success_at=None,
            seconds_since_last=None,
            max_gap_seconds=9000,
        )
        out = format_report(self._data(gaps=[gap]))
        assert "treasury_press" in out
        assert "never produced" in out
        assert "✅ No gaps." not in out

    def test_message_fits_telegram_4096_limit(self) -> None:
        # Build a pessimistic case: every collector slug and a long gap list.
        many_rows = [_sr(f"source_{i:02d}_long_slug", ins=999, dup=999, seen=999) for i in range(20)]
        many_gaps = [
            Gap(f"src_{i:02d}", i, "overdue", GEN_AT, i * 60, 600)
            for i in range(20)
        ]
        data = ReportData(
            generated_at=GEN_AT,
            per_source=many_rows,
            total_articles=999_999_999,
            db_size_pretty="999 GB",
            disk=DiskUsage(100 * 1024**3, 99 * 1024**3, 1 * 1024**3),
            gaps=many_gaps,
        )
        out = format_report(data)
        assert len(out) < 4096


def _mock_httpx_client(handler):
    """Return a patcher that replaces httpx.Client with one using MockTransport."""
    real_client_cls = httpx.Client
    transport = httpx.MockTransport(handler)

    def factory(**kwargs):
        kwargs.pop("transport", None)
        return real_client_cls(transport=transport, **kwargs)

    return patch("httpx.Client", factory)


class TestSendTelegram:
    def test_posts_expected_payload(self) -> None:
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["json"] = request.content.decode()
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

        with _mock_httpx_client(handler):
            body = send_telegram(
                "<b>hi</b>",
                bot_token="TESTTOKEN",
                chat_id="123456",
            )
        assert body["ok"] is True
        assert "bot" in captured["url"]
        assert "TESTTOKEN" in captured["url"]
        import json
        body_json = json.loads(captured["json"])
        assert body_json["chat_id"] == "123456"
        assert body_json["parse_mode"] == "HTML"
        assert body_json["disable_web_page_preview"] is True

    def test_raises_on_api_not_ok(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": False, "description": "chat not found"})

        with _mock_httpx_client(handler):
            with pytest.raises(RuntimeError, match="telegram sendMessage failed"):
                send_telegram("hi", bot_token="x", chat_id="y")

    def test_raises_on_http_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"ok": False, "description": "unauthorized"})

        with _mock_httpx_client(handler):
            with pytest.raises(httpx.HTTPStatusError):
                send_telegram("hi", bot_token="x", chat_id="y")
