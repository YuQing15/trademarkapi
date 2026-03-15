from __future__ import annotations

import html
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable


DEFAULT_SEARCH_URL_TEMPLATE = "https://trademarks.ipo.gov.uk/ipo-tmtext/page/Results/1/{query}"
DEFAULT_DETAIL_URL_TEMPLATE = "https://trademarks.ipo.gov.uk/ipo-tmcase/page/Results/1/{reg_no}"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

_ENTRY_LINK_RE = re.compile(
    r'href="(?P<href>[^"]*?/ipo-tmcase/page/Results/1/(?P<reg>[^"#?]+)[^"]*)"',
    re.IGNORECASE,
)


def norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _strip_tags(html_text: str) -> str:
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html_text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return _collapse_whitespace(html.unescape(text))


def _extract_label(text: str, label: str, stop_labels: list[str]) -> str:
    label_re = re.escape(label)
    stop_re = "|".join(re.escape(x) for x in stop_labels)
    pattern = rf"{label_re}\s*(?P<value>.*?)(?=(?:{stop_re})|$)"
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return ""
    return _collapse_whitespace(match.group("value"))


def _parse_uk_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    for fmt in ("%d %B %Y", "%d %b %Y", "%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return raw


def _parse_class_codes(value: str) -> list[str]:
    return re.findall(r"\d+", value or "")


@dataclass
class UKIPOFallbackMark:
    reg_no: str
    mark_text: str
    owner_name: str
    country: str
    status: str
    category: str
    mark_type: str
    filed: str
    published: str
    registered: str
    expired: str
    renewal_due: str
    class_codes: list[str]
    goods_services: str
    source_url: str

    def as_cache_row(self) -> dict[str, str]:
        return {
            "reg_no": self.reg_no,
            "mark_text": self.mark_text,
            "mark_text_norm": norm_text(self.mark_text),
            "owner_name": self.owner_name,
            "country": self.country,
            "status": self.status,
            "category": self.category,
            "mark_type": self.mark_type,
            "filed": self.filed,
            "published": self.published,
            "registered": self.registered,
            "expired": self.expired,
            "renewal_due": self.renewal_due,
            "class_codes": ",".join(self.class_codes),
            "goods_services": self.goods_services,
            "source_url": self.source_url,
        }


class UKIPOFallbackService:
    """Best-effort live lookup against the public UKIPO trademark search site.

    This service is intentionally isolated from the local DB search path so it can
    be removed once a licensed full register feed is available.
    """

    def __init__(
        self,
        timeout_seconds: int = 10,
        search_url_template: str = DEFAULT_SEARCH_URL_TEMPLATE,
        detail_url_template: str = DEFAULT_DETAIL_URL_TEMPLATE,
        fetcher: Callable[[str, int], str] | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.search_url_template = search_url_template
        self.detail_url_template = detail_url_template
        self.fetcher = fetcher or self._fetch

    def search_word_mark(self, term: str, limit: int = 10) -> list[UKIPOFallbackMark]:
        query = urllib.parse.quote(term.strip())
        search_url = self.search_url_template.format(query=query)
        html_text = self.fetcher(search_url, self.timeout_seconds)
        results = self._parse_results_page(html_text, limit=limit)
        enriched: list[UKIPOFallbackMark] = []
        for item in results:
            try:
                detail_html = self.fetcher(item.source_url, self.timeout_seconds)
                enriched.append(self._merge_detail(item, detail_html))
            except Exception:
                enriched.append(item)
        return enriched

    def _fetch(self, url: str, timeout_seconds: int) -> str:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
                "Cache-Control": "no-cache",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            content_type = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(content_type, errors="replace")

    def _parse_results_page(self, html_text: str, limit: int) -> list[UKIPOFallbackMark]:
        matches = list(_ENTRY_LINK_RE.finditer(html_text))
        if not matches:
            return []

        results: list[UKIPOFallbackMark] = []
        for idx, match in enumerate(matches[:limit]):
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(html_text)
            block = html_text[start:end]
            text = _strip_tags(block)
            reg_no = _collapse_whitespace(html.unescape(match.group("reg")))
            href = html.unescape(match.group("href"))
            if href.startswith("/"):
                href = "https://trademarks.ipo.gov.uk" + href

            stop_labels = ["Status:", "Mark text:", "File date:", "Classes:", "Owner:", "Trade mark type:"]
            status = _extract_label(text, "Status:", ["Mark text:", "File date:", "Classes:", "Trade mark type:"])
            mark_text = _extract_label(text, "Mark text:", ["File date:", "Classes:", "Trade mark type:", "Status:"])
            filed = _parse_uk_date(_extract_label(text, "File date:", ["Classes:", "Trade mark type:", "Status:", "Mark text:"]))
            classes = _parse_class_codes(_extract_label(text, "Classes:", ["Trade mark type:", "Status:", "Mark text:", "File date:"]))
            mark_type = _extract_label(text, "Trade mark type:", stop_labels)

            results.append(
                UKIPOFallbackMark(
                    reg_no=reg_no,
                    mark_text=mark_text,
                    owner_name="",
                    country="United Kingdom",
                    status=status,
                    category="",
                    mark_type=mark_type,
                    filed=filed,
                    published="",
                    registered="",
                    expired="",
                    renewal_due="",
                    class_codes=classes,
                    goods_services="",
                    source_url=href,
                )
            )

        return results

    def _merge_detail(self, base: UKIPOFallbackMark, detail_html: str) -> UKIPOFallbackMark:
        text = _strip_tags(detail_html)
        stop_labels = [
            "Status:",
            "Mark text:",
            "File date:",
            "Classes:",
            "Trade mark type:",
            "Holder:",
            "Applicant name:",
            "Proprietor:",
            "Owner:",
            "Goods and services:",
            "Published:",
            "Registration date:",
            "Renewal due date:",
            "Expiry date:",
            "Category of mark:",
        ]

        def first_label(labels: list[str], stops: list[str]) -> str:
            for label in labels:
                value = _extract_label(text, label, stops)
                if value:
                    return value
            return ""

        owner_name = first_label(
            ["Holder:", "Applicant name:", "Proprietor:", "Owner:"],
            ["Address:", "Mark text:", "Goods and services:", "Classes:", "Status:", "Trade mark type:"],
        )
        goods_services = first_label(
            ["Goods and services:", "List of goods and services:"],
            ["Vienna classification:", "Representative:", "Address:", "Last renewal date:", "Renewal due date:"],
        )
        category = first_label(["Category of mark:"], stop_labels)
        mark_type = first_label(["Trade mark type:", "Mark type:"], stop_labels) or base.mark_type
        published = _parse_uk_date(first_label(["Published:", "Publication date:"], stop_labels))
        registered = _parse_uk_date(first_label(["Registration date:", "Registered:"], stop_labels))
        renewal_due = _parse_uk_date(first_label(["Renewal due date:"], stop_labels))
        expired = _parse_uk_date(first_label(["Expiry date:", "Expired:"], stop_labels))
        classes = _parse_class_codes(first_label(["Classes:"], stop_labels)) or base.class_codes

        return UKIPOFallbackMark(
            reg_no=base.reg_no,
            mark_text=base.mark_text,
            owner_name=owner_name,
            country=base.country,
            status=base.status,
            category=category,
            mark_type=mark_type,
            filed=base.filed,
            published=published,
            registered=registered,
            expired=expired,
            renewal_due=renewal_due,
            class_codes=classes,
            goods_services=goods_services,
            source_url=base.source_url,
        )
