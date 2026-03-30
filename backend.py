from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import threading
import urllib.error
import urllib.request
from datetime import datetime, date
from pathlib import Path
from typing import Any
from difflib import SequenceMatcher

from flask import Flask, jsonify, request, send_from_directory, render_template
from flask_cors import CORS

from services.ukipo_fallback import UKIPOFallbackService

DB_PATH = Path(os.getenv("TRADEMARK_DB_PATH", "data/trademarks.sqlite"))
DB_URL = os.getenv("TRADEMARK_DB_URL", "").strip()
ENABLE_UKIPO_FALLBACK = os.getenv("ENABLE_UKIPO_FALLBACK", "0") == "1"
ENABLE_LOCAL_SIMILARITY = os.getenv("ENABLE_LOCAL_SIMILARITY", "1") == "1"
UKIPO_FALLBACK_TIMEOUT = int(os.getenv("UKIPO_FALLBACK_TIMEOUT", "10"))
UKIPO_FALLBACK_LIMIT = int(os.getenv("UKIPO_FALLBACK_LIMIT", "10"))
UKIPO_FALLBACK_CACHE_DAYS = int(os.getenv("UKIPO_FALLBACK_CACHE_DAYS", "14"))
_download_lock = threading.Lock()
_download_attempted = False

app = Flask(__name__)
ukipo_fallback_service = UKIPOFallbackService(timeout_seconds=UKIPO_FALLBACK_TIMEOUT)


def _parse_allowed_origins(raw: str) -> list[str]:
    items = [x.strip() for x in (raw or "").split(",")]
    return [x for x in items if x]


allowed_origins = _parse_allowed_origins(os.getenv("ALLOWED_ORIGINS", ""))
if allowed_origins:
    CORS(
        app,
        resources={
            r"/check": {"origins": allowed_origins},
            r"/health": {"origins": allowed_origins},
        },
    )
else:
    # Local fallback only; set ALLOWED_ORIGINS in production.
    CORS(app)


def norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_classes(s: str) -> list[str]:
    if not s:
        return []
    parts = re.split(r"[^0-9]+", s)
    return [p for p in parts if p]


def infer_owner_type(name: str) -> str:
    if not name:
        return "unknown"
    n = name.lower()
    if any(x in n for x in [" ltd", " limited", " llc", " inc", " corp", " gmbh", " plc", " llp"]):
        return "company"
    if " and " in n or " & " in n:
        return "company"
    return "individual_or_other"


def now_date() -> date:
    return datetime.utcnow().date()


def years_since(date_str: str) -> int | None:
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None
    delta = now_date() - d
    if delta.days < 0:
        return 0
    return delta.days // 365


def is_active(status: str, expired: str) -> bool:
    status_norm = (status or "").strip().lower()
    if status_norm in {"dead", "expired", "withdrawn", "revoked", "cancelled", "removed"}:
        return False
    if expired:
        try:
            exp = datetime.strptime(expired, "%Y-%m-%d").date()
            if exp < now_date():
                return False
        except ValueError:
            pass
    return True


def status_display(status: str, expired: str) -> str:
    status_norm = (status or "").strip().lower()
    if status_norm in {"dead", "expired", "withdrawn", "revoked", "cancelled", "removed"}:
        return "Closed"
    if expired:
        try:
            exp = datetime.strptime(expired, "%Y-%m-%d").date()
            if exp < now_date():
                return "Closed"
        except ValueError:
            pass
    return status or "—"


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def build_fts_query(norm: str) -> str:
    if not norm:
        return ""
    tokens = norm.split()
    return " ".join([f"{t}*" for t in tokens])


def tokenize(norm: str) -> list[str]:
    return [t for t in norm.split() if t]


COMMON_SEARCH_TOKENS = {
    "and",
    "for",
    "the",
    "with",
    "from",
    "your",
    "their",
    "this",
    "that",
    "into",
    "over",
    "under",
    "good",
    "best",
    "plus",
    "care",
    "group",
    "world",
    "global",
    "service",
    "services",
    "company",
    "limited",
}


def significant_tokens(norm: str) -> list[str]:
    tokens = []
    for token in tokenize(norm):
        if token in COMMON_SEARCH_TOKENS:
            continue
        if token not in tokens:
            tokens.append(token)

    preferred = [t for t in tokens if len(t) >= 5]
    if not preferred:
        preferred = [t for t in tokens if len(t) >= 4]
    return sorted(preferred, key=len, reverse=True)[:2]


def build_broad_fts_terms(norm: str) -> list[str]:
    """Return bounded per-token prefix queries for broader local similarity search.

    A single large OR FTS query can get expensive on the Render instance for
    common terms. Per-token queries keep the search predictable and easier to
    cap.
    """
    tokens = tokenize(norm)
    if not tokens:
        return []

    parts: list[str] = []
    for token in sorted(tokens, key=len, reverse=True):
        if len(token) >= 5:
            parts.append(f"{token[:5]}*")
        elif len(token) >= 4:
            parts.append(f"{token}*")

    seen: set[str] = set()
    unique_parts: list[str] = []
    for part in parts:
        if part not in seen:
            seen.add(part)
            unique_parts.append(part)
    return unique_parts[:1]


def allow_broad_local_similarity(term_norm: str) -> bool:
    """Keep broad local similarity search on a short leash.

    Multi-word searches and very long terms can explode the FTS candidate set on
    the small Render instance and trigger gunicorn worker timeouts. For now we
    only allow the broader local search for short single-token queries. Exact
    and prefix search still run for every term.
    """
    tokens = tokenize(term_norm)
    return len(tokens) == 1 and 4 <= len(tokens[0]) <= 12


def local_similarity_score(term_norm: str, mark_norm: str) -> float:
    if not term_norm or not mark_norm:
        return 0.0
    if term_norm == mark_norm:
        return 1.0

    seq = similarity(term_norm, mark_norm)
    term_tokens = set(tokenize(term_norm))
    mark_tokens = set(tokenize(mark_norm))
    overlap = len(term_tokens & mark_tokens) / max(len(term_tokens), 1)

    prefix_bonus = 0.0
    if mark_norm.startswith(term_norm) or term_norm.startswith(mark_norm):
        prefix_bonus = 0.18
    elif any(mt.startswith(tt[:4]) for tt in term_tokens for mt in mark_tokens if len(tt) >= 4):
        prefix_bonus = 0.12

    contains_bonus = 0.08 if term_norm in mark_norm or mark_norm in term_norm else 0.0
    return min(1.0, seq * 0.62 + overlap * 0.22 + prefix_bonus + contains_bonus)


def token_overlap_ratio(term_norm: str, mark_norm: str) -> float:
    term_tokens = set(tokenize(term_norm))
    mark_tokens = set(tokenize(mark_norm))
    if not term_tokens:
        return 0.0
    return len(term_tokens & mark_tokens) / len(term_tokens)


def is_close_phrase_match(term_norm: str, mark_norm: str, sim: float) -> bool:
    if not term_norm or not mark_norm or term_norm == mark_norm:
        return False
    overlap = token_overlap_ratio(term_norm, mark_norm)
    if term_norm.startswith(mark_norm) or mark_norm.startswith(term_norm):
        return True
    if term_norm in mark_norm or mark_norm in term_norm:
        return True
    return sim >= 0.86 and overlap >= 0.5


def open_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA case_sensitive_like=ON")
    ensure_runtime_schema(con)
    return con


def has_index() -> bool:
    return DB_PATH.exists()


def ensure_index() -> tuple[bool, str]:
    global _download_attempted

    if has_index():
        return True, ""

    if not DB_URL:
        return False, "Index not found and TRADEMARK_DB_URL is not set."

    with _download_lock:
        if has_index():
            return True, ""
        if _download_attempted and not has_index():
            return False, "Index download already attempted and failed."
        _download_attempted = True

        try:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = DB_PATH.with_suffix(".download")
            urllib.request.urlretrieve(DB_URL, tmp_path)
            os.replace(tmp_path, DB_PATH)
        except Exception as exc:
            return False, f"Failed to download index from TRADEMARK_DB_URL: {exc}"

    return (True, "") if has_index() else (False, "Index download completed but file not found.")


def ensure_runtime_schema(con: sqlite3.Connection) -> None:
    """Runtime-only schema for fallback caching.

    The XML/offline journal build remains the primary source of truth. This table
    only stores backend fallback hits so future searches do not need to re-query
    UKIPO for the same exact term.
    """

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fallback_cache (
            id INTEGER PRIMARY KEY,
            query_text TEXT NOT NULL,
            query_norm TEXT NOT NULL,
            reg_no TEXT NOT NULL,
            mark_text TEXT,
            mark_text_norm TEXT,
            owner_name TEXT,
            country TEXT NOT NULL DEFAULT 'United Kingdom',
            status TEXT,
            category TEXT,
            mark_type TEXT,
            filed TEXT,
            published TEXT,
            registered TEXT,
            expired TEXT,
            renewal_due TEXT,
            class_codes TEXT,
            goods_services TEXT,
            source_url TEXT,
            fetched_at TEXT NOT NULL,
            UNIQUE(query_norm, reg_no)
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_fallback_query_norm ON fallback_cache(query_norm)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fallback_reg_no ON fallback_cache(reg_no)")
    con.commit()


def resolve_countries(country: str) -> list[str]:
    c = (country or "").strip().lower()
    if c in {"all", "all countries", "any"}:
        return ["United Kingdom", "European Union", "United States", "Rest of World"]
    if c in {"uk", "united kingdom", "uk only"}:
        return ["United Kingdom"]
    if c in {"eu", "european union", "eu only"}:
        return ["European Union"]
    if c in {"us", "united states", "us only"}:
        return ["United States"]
    if c in {"uk & eu", "uk and eu", "uk+eu"}:
        return ["United Kingdom", "European Union"]
    if c in {"rest of world", "row", "world"}:
        return ["Rest of World"]
    return [country]


def query_candidates(con: sqlite3.Connection, term_norm: str, country: str, limit: int = 25) -> list[sqlite3.Row]:
    countries = resolve_countries(country)
    expanded_countries: list[str] = []
    for value in countries:
        if value not in expanded_countries:
            expanded_countries.append(value)
        # Keep retrieval tolerant of the country labels currently stored in
        # the DB without changing the API contract.
        if value == "United States" and "United States of America" not in expanded_countries:
            expanded_countries.append("United States of America")

    countries = expanded_countries
    placeholders = ",".join(["?"] * len(countries))
    candidates: list[sqlite3.Row] = []
    seen_ids: set[int] = set()

    def add_rows(rows: list[sqlite3.Row]) -> None:
        for row in rows:
            row_id = row["id"]
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            candidates.append(row)

    # 1) Exact match (fast)
    rows = con.execute(
        """
        SELECT m.*
        FROM marks m
        WHERE m.country IN (""" + placeholders + """)
          AND m.mark_text_norm = ?
        LIMIT ?
        """,
        (*countries, term_norm, limit),
    ).fetchall()
    add_rows(rows)

    # 2) Prefix match only (fast, index-friendly)
    if len(term_norm) >= 4:
        like = f"{term_norm}%"
        rows = con.execute(
            """
            SELECT m.*
            FROM marks m
            WHERE m.country IN (""" + placeholders + """)
              AND m.mark_text_norm LIKE ?
            LIMIT ?
            """,
            (*countries, like, limit),
        ).fetchall()
        add_rows(rows)

    # 3) Bounded token-prefix FTS search.
    #
    # This stays cheap on Render by querying at most two significant tokens,
    # each with its own LIMIT. We avoid the broad multi-token OR queries that
    # previously caused worker timeouts.
    if ENABLE_LOCAL_SIMILARITY:
        fts_tokens = significant_tokens(term_norm)
        if not fts_tokens:
            raw_tokens = tokenize(term_norm)
            if len(raw_tokens) == 1 and len(raw_tokens[0]) >= 4:
                fts_tokens = [raw_tokens[0]]

        for token in fts_tokens:
            try:
                rows = con.execute(
                    """
                    SELECT m.*
                    FROM marks_fts f
                    JOIN marks m ON m.id = f.rowid
                    WHERE m.country IN (""" + placeholders + """)
                      AND f.mark_text MATCH ?
                    LIMIT ?
                    """,
                    (*countries, f"{token}*", max(limit, 20)),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            add_rows(rows)

    # 4) Safe case-insensitive fallback on the original mark text.
    if not candidates and term_norm:
        rows = con.execute(
            """
            SELECT m.*
            FROM marks m
            WHERE m.country IN (""" + placeholders + """)
              AND lower(m.mark_text) LIKE ?
            LIMIT ?
            """,
            (*countries, f"%{term_norm}%", limit),
        ).fetchall()
        add_rows(rows)

    if not candidates:
        return []

    ranked = sorted(
        candidates,
        key=lambda row: (
            is_active(row["status"], row["expired"]),
            local_similarity_score(term_norm, row["mark_text_norm"] or ""),
        ),
        reverse=True,
    )
    return ranked[:limit]


def _cache_is_fresh(fetched_at: str) -> bool:
    if not fetched_at:
        return False
    try:
        fetched = datetime.fromisoformat(fetched_at)
    except ValueError:
        return False
    age = datetime.utcnow() - fetched
    return age.days < UKIPO_FALLBACK_CACHE_DAYS


def query_fallback_cache(con: sqlite3.Connection, term_norm: str, limit: int = 25) -> list[sqlite3.Row]:
    rows = con.execute(
        """
        SELECT *
        FROM fallback_cache
        WHERE query_norm = ?
        ORDER BY fetched_at DESC, reg_no ASC
        LIMIT ?
        """,
        (term_norm, limit),
    ).fetchall()
    fresh_rows = [row for row in rows if _cache_is_fresh(row["fetched_at"])]
    return fresh_rows


def cache_fallback_results(con: sqlite3.Connection, query_text: str, query_norm: str, results: list[dict[str, Any]]) -> None:
    fetched_at = datetime.utcnow().isoformat(timespec="seconds")
    for item in results:
        con.execute(
            """
            INSERT INTO fallback_cache (
                query_text, query_norm, reg_no, mark_text, mark_text_norm, owner_name,
                country, status, category, mark_type, filed, published, registered,
                expired, renewal_due, class_codes, goods_services, source_url, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(query_norm, reg_no) DO UPDATE SET
                query_text = excluded.query_text,
                mark_text = excluded.mark_text,
                mark_text_norm = excluded.mark_text_norm,
                owner_name = excluded.owner_name,
                country = excluded.country,
                status = excluded.status,
                category = excluded.category,
                mark_type = excluded.mark_type,
                filed = excluded.filed,
                published = excluded.published,
                registered = excluded.registered,
                expired = excluded.expired,
                renewal_due = excluded.renewal_due,
                class_codes = excluded.class_codes,
                goods_services = excluded.goods_services,
                source_url = excluded.source_url,
                fetched_at = excluded.fetched_at
            """,
            (
                query_text,
                query_norm,
                item.get("reg_no", ""),
                item.get("mark_text", ""),
                norm_text(item.get("mark_text", "")),
                item.get("owner_name", ""),
                item.get("country", "United Kingdom"),
                item.get("status", ""),
                item.get("category", ""),
                item.get("mark_type", ""),
                item.get("filed", ""),
                item.get("published", ""),
                item.get("registered", ""),
                item.get("expired", ""),
                item.get("renewal_due", ""),
                ",".join(item.get("class_codes", []) or []),
                item.get("goods_services", ""),
                item.get("source_url", ""),
                fetched_at,
            ),
        )
    con.commit()


def fallback_allowed(country: str) -> bool:
    return ENABLE_UKIPO_FALLBACK and resolve_countries(country) == ["United Kingdom"]


def search_ukipo_fallback(term: str, limit: int = 10) -> list[dict[str, Any]]:
    results = ukipo_fallback_service.search_word_mark(term, limit=limit)
    return [item.as_cache_row() for item in results]


def query_patents(con: sqlite3.Connection, term_norm: str, limit: int = 25) -> list[sqlite3.Row]:
    # Fast prefix-only search to avoid long-running FTS scans
    if len(term_norm) < 4:
        return []

    like = f"{term_norm}%"
    rows = con.execute(
        """
        SELECT p.*
        FROM patents p
        WHERE p.applicant_name LIKE ?
           OR p.application_number LIKE ?
           OR p.publication_number LIKE ?
        LIMIT ?
        """,
        (like, like, like, limit),
    ).fetchall()
    return rows


def dedupe_mark_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    seen: set[str] = set()
    unique: list[sqlite3.Row] = []
    for row in rows:
        key = (row["reg_no"] or "").strip()
        if not key:
            key = f"row:{row['id']}"
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def score_match_conflict(match: dict[str, Any], term_norm: str, reference_classes: list[str]) -> float:
    mark_norm = norm_text(match.get("mark_text", ""))
    sim = float(match.get("similarity", 0.0))
    exact = mark_norm == term_norm
    close_phrase = is_close_phrase_match(term_norm, mark_norm, sim)
    same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
    active = bool(match.get("active"))

    score = 0.0
    if exact:
        score += 7.0
    elif sim >= 0.96:
        score += 5.5
    elif close_phrase:
        score += 4.0
    elif sim >= 0.88:
        score += 2.5
    elif sim >= 0.8:
        score += 1.2

    if same_class:
        score += 3.0
    elif reference_classes:
        score += 0.8

    if active:
        score += 2.5
    else:
        score += 0.4

    overlap = token_overlap_ratio(term_norm, mark_norm)
    score += overlap * 1.5
    return score


def score_risk(matches: list[dict[str, Any]], reference_classes: list[str], term_norm: str) -> tuple[str, str]:
    if not matches:
        return "low", "Only weak or inactive matches found"

    scored: list[tuple[float, dict[str, Any], bool, bool]] = []
    active_same_class_strong = 0
    active_cross_class_strong = 0
    moderate_matches = 0

    for match in matches[:10]:
        mark_norm = norm_text(match.get("mark_text", ""))
        sim = float(match.get("similarity", 0.0))
        exact = mark_norm == term_norm
        same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
        active = bool(match.get("active"))
        score = score_match_conflict(match, term_norm, reference_classes)
        scored.append((score, match, exact, same_class))

        if active and same_class and (exact or sim >= 0.94):
            active_same_class_strong += 1
        elif active and sim >= 0.9:
            active_cross_class_strong += 1
        elif score >= 4.5:
            moderate_matches += 1

    top_score, top_match, top_exact, top_same_class = max(scored, key=lambda item: item[0])
    total_score = sum(score for score, _, _, _ in scored)
    top_active = bool(top_match.get("active"))

    if top_exact and top_same_class and top_active:
        return "high", "Identical active mark found in the same class"
    if active_same_class_strong > 0:
        return "high", "Very similar active mark found in the same class"
    if active_same_class_strong + moderate_matches >= 2 and total_score >= 14:
        return "high", "Multiple similar active marks found"

    if active_cross_class_strong > 0 and total_score >= 8:
        return "medium", "Active similar marks found in other classes"
    if moderate_matches >= 2 or total_score >= 9:
        return "medium", "Multiple similar active marks found"
    if top_score >= 5.0 and top_active:
        return "medium", "Strong similar mark found"

    return "low", "Only weak or inactive matches found"


def country_available(con: sqlite3.Connection, country: str) -> bool:
    countries = resolve_countries(country)
    placeholders = ",".join(["?"] * len(countries))
    row = con.execute(
        "SELECT 1 FROM marks WHERE country IN (" + placeholders + ") LIMIT 1",
        (*countries,),
    ).fetchone()
    return row is not None


def summarize_mark(row: sqlite3.Row, term_norm: str) -> dict[str, Any]:
    mark_text = row["mark_text"] or ""
    mark_norm = norm_text(mark_text)
    sim = local_similarity_score(term_norm, mark_norm)

    filed = row["filed"] or ""
    registered = row["registered"] or ""
    expired = row["expired"] or ""

    return {
        "reg_no": row["reg_no"],
        "mark_text": mark_text,
        "owner_name": row["owner_name"],
        "owner_type": row["owner_type"] if "owner_type" in row.keys() else infer_owner_type(row["owner_name"] or ""),
        "country": row["country"],
        "status": row["status"],
        "status_display": status_display(row["status"], expired),
        "category": row["category"] if "category" in row.keys() else "",
        "mark_type": row["mark_type"] if "mark_type" in row.keys() else "",
        "filed": filed,
        "registered": registered,
        "expired": expired,
        "renewal_due": row["renewal_due"],
        "age_years": years_since(filed) if filed else None,
        "active": is_active(row["status"], expired),
        "class_codes": (row["class_codes"] or "").split(",") if row["class_codes"] else [],
        "goods_services": row["goods_services"] if "goods_services" in row.keys() else "",
        "source_url": row["source_url"] if "source_url" in row.keys() else "",
        "data_source": row["data_source"] if "data_source" in row.keys() else "local_database",
        "similarity": round(sim, 4),
    }


def determine_reference_classes(matches: list[dict[str, Any]], class_filter: list[str], term_norm: str) -> list[str]:
    reference_classes = list(class_filter)
    if reference_classes:
        return reference_classes

    exact = next((m for m in matches if norm_text(m.get("mark_text", "")) == term_norm), None)
    if exact:
        return list(exact.get("class_codes", []) or [])
    return []


def rank_mark(match: dict[str, Any], term_norm: str, reference_classes: list[str]) -> tuple:
    mark_norm = norm_text(match.get("mark_text", ""))
    sim = float(match.get("similarity", 0.0))
    exact = mark_norm == term_norm
    close_phrase = is_close_phrase_match(term_norm, mark_norm, sim)
    same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
    overlap = token_overlap_ratio(term_norm, mark_norm)
    return (
        1 if exact else 0,
        1 if close_phrase else 0,
        1 if same_class else 0,
        1 if match.get("active") else 0,
        sim,
        overlap,
    )


def prioritize_exact_matches(matches: list[dict[str, Any]], term_norm: str, reference_classes: list[str]) -> list[dict[str, Any]]:
    def key(match: dict[str, Any]) -> tuple:
        mark_norm = norm_text(match.get("mark_text", ""))
        exact = mark_norm == term_norm
        same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
        return (
            1 if exact and same_class else 0,
            1 if exact else 0,
            rank_mark(match, term_norm, reference_classes),
        )

    return sorted(matches, key=key, reverse=True)


def split_mark_groups(matches: list[dict[str, Any]], reference_classes: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ref_set = set(reference_classes)

    chosen_class_matches: list[dict[str, Any]] = []
    cross_class_matches: list[dict[str, Any]] = []

    for match in matches:
        match_classes = set(match.get("class_codes", []))
        if ref_set and match_classes & ref_set:
            chosen_class_matches.append(match)
        else:
            cross_class_matches.append(match)

    return chosen_class_matches, cross_class_matches


def patent_active(status: str, date_not_in_force: str) -> bool:
    s = (status or "").strip().lower()
    if date_not_in_force:
        return False
    if any(x in s for x in ["lapsed", "ceased", "withdrawn", "revoked"]):
        return False
    return True


def summarize_patent(row: sqlite3.Row, term_norm: str) -> dict[str, Any]:
    def safe(v: str) -> str:
        return v or ""

    term = term_norm or ""
    applicant = safe(row["applicant_name"])
    sim = similarity(term, norm_text(applicant)) if applicant else 0.0
    filed = safe(row["filing_date"])
    return {
        "application_number": row["application_number"],
        "publication_number": row["publication_number"],
        "applicant_name": applicant,
        "applicant_country": row["applicant_country"] or row["applicant_country_code"],
        "ipc7": row["ipc7"],
        "ipc8": row["ipc8"],
        "status": row["status"],
        "filing_date": filed,
        "earliest_filing_date": row["earliest_filing_date"],
        "publication_a_date": row["publication_a_date"],
        "publication_b_date": row["publication_b_date"],
        "last_renewal_date": row["last_renewal_date"],
        "date_not_in_force": row["date_not_in_force"],
        "reason_not_in_force": row["reason_not_in_force"],
        "active": patent_active(row["status"], row["date_not_in_force"]),
        "age_years": years_since(filed) if filed else None,
        "similarity": round(sim, 4),
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/check", methods=["POST"])
def check():
    ok, msg = ensure_index()
    if not ok:
        return jsonify({"error": msg}), 400

    payload = request.get_json(force=True) or {}
    term = (payload.get("trademark") or "").strip()
    country = (payload.get("country") or "").strip()
    class_filter = parse_classes(payload.get("classes") or "")
    include_patents = bool(payload.get("include_patents", True))

    if not term:
        return jsonify({"error": "Missing trademark"}), 400
    if not country:
        return jsonify({"error": "Missing country"}), 400
    if len(term.strip()) < 3:
        return jsonify({"error": "Please enter at least 3 characters."}), 400

    term_norm = norm_text(term)
    con = open_db()
    if not country_available(con, country) and not fallback_allowed(country):
        con.close()
        return jsonify(
            {
                "error": "No records found for this country in the current index.",
                "country": country,
            }
        ), 400
    rows = query_candidates(con, term_norm, country)
    result_source = "local_database"
    fallback_used = False
    fallback_error = ""
    warnings: list[str] = []

    if not rows and fallback_allowed(country):
        cached_rows = query_fallback_cache(con, term_norm, limit=UKIPO_FALLBACK_LIMIT)
        if cached_rows:
            rows = cached_rows
            result_source = "ukipo_fallback_cache"
            fallback_used = True
        else:
            try:
                fallback_results = search_ukipo_fallback(term, limit=UKIPO_FALLBACK_LIMIT)
                if fallback_results:
                    cache_fallback_results(con, term, term_norm, fallback_results)
                    rows = query_fallback_cache(con, term_norm, limit=UKIPO_FALLBACK_LIMIT)
                    result_source = "ukipo_fallback"
                    fallback_used = True
            except urllib.error.HTTPError as exc:
                fallback_error = f"UKIPO fallback request failed: HTTP {exc.code}"
            except Exception as exc:
                fallback_error = f"UKIPO fallback request failed: {exc}"

    patent_rows = query_patents(con, term_norm) if include_patents else []
    con.close()

    rows = dedupe_mark_rows(rows)
    matches = [summarize_mark(r, term_norm) for r in rows]
    for match in matches:
        match["data_source"] = result_source if result_source != "local_database" else "local_database"

    reference_classes = determine_reference_classes(matches, class_filter, term_norm)
    matches = prioritize_exact_matches(matches, term_norm, reference_classes)

    # Keep top 50
    matches = matches[:50]
    chosen_class_matches, cross_class_matches = split_mark_groups(matches, reference_classes)
    chosen_class_matches = prioritize_exact_matches(chosen_class_matches, term_norm, reference_classes)
    cross_class_matches = prioritize_exact_matches(cross_class_matches, term_norm, reference_classes)
    matches = chosen_class_matches + cross_class_matches

    risk, risk_explanation = score_risk(matches, reference_classes, term_norm)

    patents = [summarize_patent(r, term_norm) for r in patent_rows]
    patents.sort(key=lambda p: (p["active"], p["similarity"]), reverse=True)
    patents = patents[:50]

    ukipo_manual_search_url = "https://trademarks.ipo.gov.uk/ipo-tmtext?reset"

    if not matches:
        result_source = "no_match"
        uk_only = resolve_countries(country) == ["United Kingdom"]
        if fallback_allowed(country) and fallback_error:
            warnings.append(
                "No local match was found, and the live UKIPO fallback is temporarily unavailable. "
                "Please try again later or verify directly on the UKIPO register."
            )
        elif fallback_allowed(country):
            warnings.append(
                "No matches were found in the UK. Similar marks may still exist in other countries."
            )
        else:
            warnings.append(
                "No matches were found in the UK. Similar marks may still exist in other countries."
                if uk_only
                else "No local match was found for this exact term."
            )

    return jsonify(
        {
            "trademark": term,
            "country": country,
            "classes": class_filter,
            "risk_level": risk,
            "risk_explanation": risk_explanation,
            "result_source": result_source,
            "fallback_used": fallback_used,
            "fallback_error": fallback_error,
            "warnings": warnings,
            "ukipo_manual_search_url": ukipo_manual_search_url,
            "ukipo_manual_search_term": term,
            "reference_classes": reference_classes,
            "match_count": len(matches),
            "patent_count": len(patents),
            "notes": [
                "Usage is inferred from status/expiry fields in the dataset; it is not verified market use.",
                "Owner business type is not provided by the dataset; owner_type is inferred from the owner name.",
            ],
            "similar_marks": matches,
            "chosen_class_matches": chosen_class_matches,
            "cross_class_matches": cross_class_matches,
            "patents": patents,
        }
    )


@app.route("/health")
def health():
    ok, msg = ensure_index()
    return jsonify(
        {
            "ok": True,
            "index": ok,
            "db_path": str(DB_PATH),
            "db_url_configured": bool(DB_URL),
            "ukipo_fallback_enabled": ENABLE_UKIPO_FALLBACK,
            "local_similarity_enabled": ENABLE_LOCAL_SIMILARITY,
            "message": msg,
        }
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
