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
from time import perf_counter
from typing import Any
from difflib import SequenceMatcher

from flask import Flask, jsonify, request, send_from_directory, render_template
from flask_cors import CORS

from services.ukipo_fallback import UKIPOFallbackService

DB_PATH = Path(os.getenv("TRADEMARK_DB_PATH", "data/trademarks.sqlite"))
DB_URL = os.getenv("TRADEMARK_DB_URL", "").strip()
SUPPLEMENTAL_MARKS_PATH = Path(os.getenv("SUPPLEMENTAL_MARKS_PATH", "data/supplemental_marks.json"))
ENABLE_UKIPO_FALLBACK = os.getenv("ENABLE_UKIPO_FALLBACK", "0") == "1"
ENABLE_LOCAL_SIMILARITY = os.getenv("ENABLE_LOCAL_SIMILARITY", "1") == "1"
UKIPO_FALLBACK_TIMEOUT = int(os.getenv("UKIPO_FALLBACK_TIMEOUT", "10"))
UKIPO_FALLBACK_LIMIT = int(os.getenv("UKIPO_FALLBACK_LIMIT", "10"))
UKIPO_FALLBACK_CACHE_DAYS = int(os.getenv("UKIPO_FALLBACK_CACHE_DAYS", "14"))
MAX_SQL_CANDIDATES = max(20, int(os.getenv("MAX_SQL_CANDIDATES", "40")))
MAX_PYTHON_SCORE_ROWS = max(5, int(os.getenv("MAX_PYTHON_SCORE_ROWS", "15")))
HIGH_SIMILARITY_CUTOFF = float(os.getenv("HIGH_SIMILARITY_CUTOFF", "0.9"))
MAX_RETURNED_MATCHES = max(10, int(os.getenv("MAX_RETURNED_MATCHES", "25")))
DEFAULT_SIMILAR_LIMIT = max(1, int(os.getenv("DEFAULT_SIMILAR_LIMIT", "25")))
MAX_SIMILAR_LIMIT = max(DEFAULT_SIMILAR_LIMIT, int(os.getenv("MAX_SIMILAR_LIMIT", "25")))
ENABLE_STARTUP_WARMUP = os.getenv("ENABLE_STARTUP_WARMUP", "1") == "1"
MARK_LIGHT_SELECT = """
    m.id,
    m.reg_no,
    m.mark_text,
    m.mark_text_norm,
    m.owner_name,
    m.owner_type,
    m.country,
    m.status,
    m.category,
    m.mark_type,
    m.filed,
    m.registered,
    m.expired,
    m.renewal_due,
    m.class_codes
"""
_download_lock = threading.Lock()
_download_attempted = False
_supplemental_marks_cache: list[dict[str, Any]] | None = None
_supplemental_marks_mtime: float | None = None
_runtime_schema_lock = threading.Lock()
_runtime_schema_mtime: float | None = None
_warmup_lock = threading.Lock()
_warmup_started = False

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


def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("’", "'")
    text = text.replace("'", "")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def norm_text(s: str) -> str:
    return normalize_text(s)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def normalized_mark_sql(column: str = "m.mark_text") -> str:
    expr = f"lower(coalesce({column}, ''))"
    for old, new in [
        ("’", "'"),
        ("'", ""),
        ("-", " "),
        (".", " "),
        (",", " "),
        ("/", " "),
        ("&", " "),
        ("(", " "),
        (")", " "),
        (":", " "),
        (";", " "),
        ('"', " "),
    ]:
        expr = f"replace({expr}, {_sql_literal(old)}, {_sql_literal(new)})"
    for _ in range(4):
        expr = f"replace({expr}, '  ', ' ')"
    return f"trim({expr})"


def db_norm_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def needs_runtime_normalized_search(term: str) -> bool:
    raw = (term or "").strip()
    if not raw:
        return False
    lowered = re.sub(r"\s+", " ", raw.lower()).strip()
    return normalize_text(raw) != lowered


def search_norm_variants(term: str, term_norm: str) -> list[str]:
    variants: list[str] = []
    for candidate in [term_norm, db_norm_text(term)]:
        if candidate and candidate not in variants:
            variants.append(candidate)

    # Keep search aligned with existing stored mark_text_norm values that were
    # built by replacing punctuation with spaces. This lets plain inputs like
    # "gails" also match stored norms such as "gail s" without rebuilding the DB.
    extra_variants: list[str] = []
    for candidate in list(variants):
        if " " not in candidate and len(candidate) > 2 and candidate.endswith("s"):
            extra_variants.append(candidate[:-1] + " s")
        tokens = candidate.split()
        for i, token in enumerate(tokens):
            if len(token) > 2 and token.endswith("s"):
                split_tokens = tokens[:]
                split_tokens[i] = token[:-1]
                split_tokens.insert(i + 1, "s")
                extra_variants.append(" ".join(split_tokens))

    for candidate in extra_variants:
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def prefix_upper_bound(prefix: str) -> str:
    return prefix + "\uffff"


def parse_classes(s: str) -> list[str]:
    if not s:
        return []
    parts = re.split(r"[^0-9]+", s)
    return [p for p in parts if p]


def parse_pagination_value(value: Any, default: int, minimum: int = 0, maximum: int = 100) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


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


def damerau_levenshtein_distance(a: str, b: str, max_distance: int = 3) -> int:
    if a == b:
        return 0
    if not a or not b:
        return max(len(a), len(b))
    if abs(len(a) - len(b)) > max_distance:
        return max_distance + 1

    prev_prev = None
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        row_min = curr[0]
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            value = min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost,
            )
            if (
                prev_prev is not None
                and i > 1
                and j > 1
                and ca == b[j - 2]
                and a[i - 2] == cb
            ):
                value = min(value, prev_prev[j - 2] + 1)
            curr.append(value)
            if value < row_min:
                row_min = value
        if row_min > max_distance:
            return max_distance + 1
        prev_prev, prev = prev, curr
    return prev[-1]


def typo_similarity(term_norm: str, mark_norm: str) -> float:
    term_tokens = tokenize(term_norm)
    mark_tokens = tokenize(mark_norm)
    if len(term_tokens) != 1 or len(mark_tokens) != 1:
        return 0.0
    a = term_tokens[0]
    b = mark_tokens[0]
    if min(len(a), len(b)) < 5:
        return 0.0
    distance = damerau_levenshtein_distance(a, b, max_distance=3)
    if distance > 3:
        return 0.0
    return max(0.0, 1.0 - (distance / max(len(a), len(b), 1)))


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


def build_candidate_prefix_terms(norm: str) -> list[str]:
    terms: list[str] = []
    filtered_tokens: list[str] = []
    for token in tokenize(norm):
        if token in COMMON_SEARCH_TOKENS:
            continue
        if len(token) < 4:
            continue
        if token not in filtered_tokens:
            filtered_tokens.append(token)

    if not filtered_tokens:
        filtered_tokens = significant_tokens(norm)

    ordered_tokens = filtered_tokens[:2]
    for token in ordered_tokens:
        candidates = [token]
        if len(token) >= 5:
            candidates.append(token[:4])
        for candidate in candidates:
            if len(candidate) < 4:
                continue
            if candidate not in terms:
                terms.append(candidate)
    return terms[:4]


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
    typo_sim = typo_similarity(term_norm, mark_norm)
    term_tokens = set(tokenize(term_norm))
    mark_tokens = set(tokenize(mark_norm))
    overlap = len(term_tokens & mark_tokens) / max(len(term_tokens), 1)

    prefix_bonus = 0.0
    if mark_norm.startswith(term_norm) or term_norm.startswith(mark_norm):
        prefix_bonus = 0.18
    elif any(mt.startswith(tt[:4]) for tt in term_tokens for mt in mark_tokens if len(tt) >= 4):
        prefix_bonus = 0.12

    contains_bonus = 0.08 if term_norm in mark_norm or mark_norm in term_norm else 0.0
    typo_bonus = 0.0
    if typo_sim >= 0.7:
        typo_bonus = 0.18
    base = max(seq, typo_sim * 0.98)
    return min(1.0, base * 0.62 + overlap * 0.22 + prefix_bonus + contains_bonus + typo_bonus)


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
    if typo_similarity(term_norm, mark_norm) >= 0.82:
        return True
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


def run_lightweight_warmup() -> float:
    ok, msg = ensure_index()
    if not ok:
        raise RuntimeError(msg)

    started = perf_counter()
    con = open_db()
    try:
        country_available(con, "United Kingdom")
        query_exact_candidates(con, "microsoft", normalize_text("microsoft"), "United Kingdom", limit=1)
        query_related_prefix_candidates(con, "micro", "United Kingdom", limit=1)
    finally:
        con.close()
    return (perf_counter() - started) * 1000


def warm_search_paths() -> None:
    try:
        elapsed_ms = run_lightweight_warmup()
        app.logger.info("Startup warm-up completed in %.1fms", elapsed_ms)
    except Exception as exc:
        app.logger.warning("Startup warm-up failed: %s", exc)


def start_background_warmup() -> None:
    global _warmup_started
    if not ENABLE_STARTUP_WARMUP:
        return
    with _warmup_lock:
        if _warmup_started:
            return
        _warmup_started = True
    threading.Thread(target=warm_search_paths, daemon=True).start()


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

    global _runtime_schema_mtime

    db_mtime = DB_PATH.stat().st_mtime if DB_PATH.exists() else None
    if _runtime_schema_mtime == db_mtime:
        return

    with _runtime_schema_lock:
        db_mtime = DB_PATH.stat().st_mtime if DB_PATH.exists() else None
        if _runtime_schema_mtime == db_mtime:
            return

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
        con.execute("CREATE INDEX IF NOT EXISTS idx_marks_mark_norm ON marks(mark_text_norm)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_marks_country_mark_text_norm ON marks(country, mark_text_norm)"
        )
        con.commit()
        _runtime_schema_mtime = db_mtime


def normalize_supplemental_record(item: dict[str, Any]) -> dict[str, Any] | None:
    mark_text = (item.get("mark_text") or "").strip()
    reg_no = (item.get("reg_no") or "").strip()
    if not mark_text or not reg_no:
        return None

    owner_name = (item.get("owner_name") or "").strip()
    class_codes = item.get("class_codes") or []
    if isinstance(class_codes, str):
        class_codes = [c.strip() for c in class_codes.split(",") if c.strip()]
    elif isinstance(class_codes, list):
        class_codes = [str(c).strip() for c in class_codes if str(c).strip()]
    else:
        class_codes = []

    country = (item.get("country") or "United Kingdom").strip() or "United Kingdom"
    source_url = (item.get("source_url") or "").strip()
    if not source_url and reg_no:
        source_url = f"https://trademarks.ipo.gov.uk/ipo-tmcase/page/Results/1/{reg_no}"

    return {
        "reg_no": reg_no,
        "mark_text": mark_text,
        "mark_text_norm": norm_text(mark_text),
        "owner_name": owner_name,
        "owner_type": (item.get("owner_type") or infer_owner_type(owner_name)).strip(),
        "country": country,
        "status": (item.get("status") or "").strip(),
        "category": (item.get("category") or "").strip(),
        "mark_type": (item.get("mark_type") or "Word").strip() or "Word",
        "filed": (item.get("filed") or "").strip(),
        "registered": (item.get("registered") or "").strip(),
        "expired": (item.get("expired") or "").strip(),
        "renewal_due": (item.get("renewal_due") or "").strip(),
        "class_codes": class_codes,
        "goods_services": (item.get("goods_services") or "").strip(),
        "source_url": source_url,
    }


def load_supplemental_marks() -> list[dict[str, Any]]:
    global _supplemental_marks_cache, _supplemental_marks_mtime

    if not SUPPLEMENTAL_MARKS_PATH.exists():
        _supplemental_marks_cache = []
        _supplemental_marks_mtime = None
        return []

    mtime = SUPPLEMENTAL_MARKS_PATH.stat().st_mtime
    if _supplemental_marks_cache is not None and _supplemental_marks_mtime == mtime:
        return _supplemental_marks_cache

    try:
        payload = json.loads(SUPPLEMENTAL_MARKS_PATH.read_text(encoding="utf-8"))
    except Exception:
        _supplemental_marks_cache = []
        _supplemental_marks_mtime = mtime
        return []

    records = payload if isinstance(payload, list) else payload.get("marks", [])
    cleaned: list[dict[str, Any]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        normalized = normalize_supplemental_record(item)
        if not normalized:
            continue
        cleaned.append(normalized)

    _supplemental_marks_cache = cleaned
    _supplemental_marks_mtime = mtime
    return cleaned


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


def expanded_countries_for_query(country: str) -> list[str]:
    countries = resolve_countries(country)
    expanded_countries: list[str] = []
    for value in countries:
        if value not in expanded_countries:
            expanded_countries.append(value)
        if value == "United States" and "United States of America" not in expanded_countries:
            expanded_countries.append("United States of America")
    return expanded_countries


def query_exact_candidates(
    con: sqlite3.Connection,
    term: str,
    term_norm: str,
    country: str,
    limit: int = 25,
) -> tuple[list[sqlite3.Row], dict[str, float]]:
    countries = expanded_countries_for_query(country)
    placeholders = ",".join(["?"] * len(countries))
    variants = search_norm_variants(term, term_norm)
    candidates: list[sqlite3.Row] = []
    seen_ids: set[int] = set()
    timings = {"exact_sql_ms": 0.0, "punctuation_exact_ms": 0.0}

    for variant in variants:
        start = perf_counter()
        rows = con.execute(
            f"""
            SELECT {MARK_LIGHT_SELECT}
            FROM marks m
            WHERE m.country IN ({placeholders})
              AND m.mark_text_norm = ?
            LIMIT ?
            """,
            (*countries, variant, min(limit, 20)),
        ).fetchall()
        elapsed_ms = (perf_counter() - start) * 1000
        if variant == term_norm:
            timings["exact_sql_ms"] += elapsed_ms
        else:
            timings["punctuation_exact_ms"] += elapsed_ms
        for row in rows:
            row_id = row["id"]
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            candidates.append(row)

    return candidates[:limit], timings


def query_related_prefix_candidates(
    con: sqlite3.Connection,
    term_norm: str,
    country: str,
    limit: int = 15,
) -> tuple[list[sqlite3.Row], float]:
    countries = expanded_countries_for_query(country)
    placeholders = ",".join(["?"] * len(countries))
    if len(term_norm) < 4:
        return [], 0.0

    upper = prefix_upper_bound(term_norm)
    start = perf_counter()
    rows = con.execute(
        f"""
        SELECT {MARK_LIGHT_SELECT}
        FROM marks m
        WHERE m.country IN ({placeholders})
          AND m.mark_text_norm >= ?
          AND m.mark_text_norm < ?
        LIMIT ?
        """,
        (*countries, term_norm, upper, min(limit, 20)),
    ).fetchall()
    return rows, (perf_counter() - start) * 1000


def query_candidates(
    con: sqlite3.Connection,
    term: str,
    term_norm: str,
    country: str,
    limit: int = 25,
    skip_exact_search: bool = False,
) -> tuple[list[sqlite3.Row], dict[str, float]]:
    countries = expanded_countries_for_query(country)
    placeholders = ",".join(["?"] * len(countries))
    variants = search_norm_variants(term, term_norm)
    punctuation_fast_path = needs_runtime_normalized_search(term)
    candidates: list[sqlite3.Row] = []
    seen_ids: set[int] = set()
    multi_word_query = len(tokenize(term_norm)) > 1
    max_candidates = 60 if multi_word_query else min(max(limit, 1), MAX_SQL_CANDIDATES)
    timings = {
        "exact_sql_ms": 0.0,
        "punctuation_exact_ms": 0.0,
        "punctuation_prefix_ms": 0.0,
        "fts_ms": 0.0,
        "python_scoring_ms": 0.0,
    }
    token_prefix_terms = build_candidate_prefix_terms(term_norm)
    shared_phrase_tokens = [t for t in tokenize(term_norm) if len(t) >= 4 and t not in COMMON_SEARCH_TOKENS][:2]
    whole_prefix_limit = 12 if punctuation_fast_path else 20
    token_prefix_limit = 10 if punctuation_fast_path else (8 if len(shared_phrase_tokens) > 1 else 15)
    phrase_prefix_limit = 25 if len(shared_phrase_tokens) > 1 else 8
    phrase_prefix_offsets = (0, 25) if len(shared_phrase_tokens) > 1 else (0,)
    fts_limit = 8 if punctuation_fast_path else 10

    def add_rows(rows: list[sqlite3.Row]) -> None:
        for row in rows:
            if len(candidates) >= max_candidates:
                break
            row_id = row["id"]
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            candidates.append(row)

    def rank_rows(rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
        start = perf_counter()
        shortlist = rows[: min(len(rows), max(MAX_PYTHON_SCORE_ROWS, 30 if not multi_word_query else 60))]
        ranked = sorted(
            shortlist,
            key=lambda row: (
                is_active(row["status"], row["expired"]),
                len(tokenize(row["mark_text_norm"] or "")) > 1 if multi_word_query else False,
                row["mark_text_norm"] == term_norm,
                token_overlap_ratio(term_norm, row["mark_text_norm"] or ""),
                local_similarity_score(term_norm, row["mark_text_norm"] or ""),
            ),
            reverse=True,
        )
        timings["python_scoring_ms"] += (perf_counter() - start) * 1000
        return ranked[: (60 if multi_word_query else limit)]

    def has_high_similarity(rows: list[sqlite3.Row]) -> bool:
        for row in rows[:MAX_PYTHON_SCORE_ROWS]:
            if local_similarity_score(term_norm, row["mark_text_norm"] or "") >= HIGH_SIMILARITY_CUTOFF:
                return True
        return False

    # 1) Exact normalized match (fast, index-friendly)
    if not skip_exact_search:
        for variant in variants:
            start = perf_counter()
            rows = con.execute(
                f"""
                SELECT {MARK_LIGHT_SELECT}
                FROM marks m
                WHERE m.country IN ({placeholders})
                  AND m.mark_text_norm = ?
                LIMIT ?
                """,
                (*countries, variant, min(limit, max_candidates)),
            ).fetchall()
            elapsed_ms = (perf_counter() - start) * 1000
            if variant == term_norm:
                timings["exact_sql_ms"] += elapsed_ms
            else:
                timings["punctuation_exact_ms"] += elapsed_ms
            add_rows(rows)
            if candidates:
                return rank_rows(candidates), timings

    # 2) Prefix match only (fast, index-friendly)
    for variant in variants:
        if len(variant) < 4:
            continue
        upper = prefix_upper_bound(variant)
        start = perf_counter()
        rows = con.execute(
            f"""
            SELECT {MARK_LIGHT_SELECT}
            FROM marks m
            WHERE m.country IN ({placeholders})
              AND m.mark_text_norm >= ?
              AND m.mark_text_norm < ?
            LIMIT ?
            """,
            (*countries, variant, upper, min(max_candidates, whole_prefix_limit)),
        ).fetchall()
        timings["punctuation_prefix_ms"] += (perf_counter() - start) * 1000
        add_rows(rows)
        if candidates and has_high_similarity(candidates):
            return rank_rows(candidates), timings

    if punctuation_fast_path and candidates:
        return rank_rows(candidates), timings

    if punctuation_fast_path:
        return [], timings

    # 3) For multi-word phrases, pull in a small set of phrase marks sharing key tokens.
    if len(shared_phrase_tokens) > 1:
        for token in shared_phrase_tokens:
            upper = prefix_upper_bound(token)
            for phrase_offset in phrase_prefix_offsets:
                start = perf_counter()
                rows = con.execute(
                    f"""
                    SELECT {MARK_LIGHT_SELECT}
                    FROM marks m
                    WHERE m.country IN ({placeholders})
                      AND m.mark_text_norm >= ?
                      AND m.mark_text_norm < ?
                      AND instr(m.mark_text_norm, ' ') > 0
                    LIMIT ? OFFSET ?
                    """,
                    (*countries, token, upper, min(max_candidates, phrase_prefix_limit), phrase_offset),
                ).fetchall()
                timings["punctuation_prefix_ms"] += (perf_counter() - start) * 1000
                add_rows(rows)
                if has_high_similarity(candidates) or len(candidates) >= max_candidates:
                    return rank_rows(candidates), timings

    # 4) Token-prefix range search for typo tolerance.
    for token_prefix in token_prefix_terms:
        upper = prefix_upper_bound(token_prefix)
        start = perf_counter()
        rows = con.execute(
            f"""
            SELECT {MARK_LIGHT_SELECT}
            FROM marks m
            WHERE m.country IN ({placeholders})
              AND m.mark_text_norm >= ?
              AND m.mark_text_norm < ?
            LIMIT ?
            """,
            (*countries, token_prefix, upper, min(max_candidates, token_prefix_limit)),
        ).fetchall()
        timings["punctuation_prefix_ms"] += (perf_counter() - start) * 1000
        add_rows(rows)
        if has_high_similarity(candidates) or len(candidates) >= max_candidates:
            return rank_rows(candidates), timings

    # 5) Bounded token-prefix FTS search.
    if ENABLE_LOCAL_SIMILARITY:
        fts_tokens = build_candidate_prefix_terms(variants[0] if variants else term_norm)
        if not fts_tokens:
            raw_tokens = tokenize(variants[0] if variants else term_norm)
            if len(raw_tokens) == 1 and len(raw_tokens[0]) >= 4:
                fts_tokens = [raw_tokens[0][:4]]

        for token in fts_tokens[:2]:
            try:
                start = perf_counter()
                rows = con.execute(
                    f"""
                    SELECT {MARK_LIGHT_SELECT}
                    FROM marks_fts f
                    JOIN marks m ON m.id = f.rowid
                    WHERE m.country IN ({placeholders})
                      AND f.mark_text MATCH ?
                    LIMIT ?
                    """,
                    (*countries, f"{token}*", min(max_candidates, fts_limit)),
                ).fetchall()
                timings["fts_ms"] += (perf_counter() - start) * 1000
            except sqlite3.OperationalError:
                rows = []
            add_rows(rows)
            if has_high_similarity(candidates) or len(candidates) >= max_candidates:
                break

    if not candidates:
        return [], timings

    return rank_rows(candidates), timings


def summarize_supplemental_mark(item: dict[str, Any], term_norm: str) -> dict[str, Any]:
    mark_text = (item.get("mark_text") or "").strip()
    mark_norm = item.get("mark_text_norm") or norm_text(mark_text)
    expired = item.get("expired", "") or ""
    filed = item.get("filed", "") or ""
    class_codes = item.get("class_codes") or []
    if isinstance(class_codes, str):
        class_codes = [c for c in class_codes.split(",") if c]

    return {
        "reg_no": item.get("reg_no", ""),
        "mark_text": mark_text,
        "owner_name": item.get("owner_name", ""),
        "owner_type": item.get("owner_type") or infer_owner_type(item.get("owner_name", "")),
        "country": item.get("country", "United Kingdom"),
        "status": item.get("status", ""),
        "status_display": status_display(item.get("status", ""), expired),
        "category": item.get("category", ""),
        "mark_type": item.get("mark_type", "Word"),
        "filed": filed,
        "registered": item.get("registered", ""),
        "expired": expired,
        "renewal_due": item.get("renewal_due", ""),
        "age_years": years_since(filed) if filed else None,
        "active": is_active(item.get("status", ""), expired),
        "class_codes": class_codes,
        "goods_services": item.get("goods_services", ""),
        "source_url": item.get("source_url", ""),
        "data_source": "supplemental_source",
        "similarity": round(local_similarity_score(term_norm, mark_norm), 4),
    }


def has_exact_or_strong_result(matches: list[dict[str, Any]], term_norm: str) -> bool:
    for match in matches:
        mark_norm = norm_text(match.get("mark_text", ""))
        sim = float(match.get("similarity", 0.0))
        if mark_norm == term_norm:
            return True
        if sim >= 0.92:
            return True
        if is_close_phrase_match(term_norm, mark_norm, sim):
            return True
    return False


def has_exact_result(matches: list[dict[str, Any]], term_norm: str) -> bool:
    return any(norm_text(match.get("mark_text", "")) == term_norm for match in matches)


def query_supplemental_candidates(
    term_norm: str,
    country: str,
    limit: int = 10,
    exact_only: bool = False,
) -> list[dict[str, Any]]:
    countries = resolve_countries(country)
    records = load_supplemental_marks()
    matches: list[dict[str, Any]] = []

    for item in records:
        item_country = item.get("country", "United Kingdom")
        if item_country not in countries:
            continue

        mark_norm = item.get("mark_text_norm") or norm_text(item.get("mark_text", ""))
        if not mark_norm:
            continue

        sim = local_similarity_score(term_norm, mark_norm)
        if exact_only:
            if mark_norm == term_norm:
                matches.append(summarize_supplemental_mark(item, term_norm))
            continue

        if mark_norm == term_norm or sim >= 0.86 or term_norm in mark_norm or mark_norm in term_norm:
            matches.append(summarize_supplemental_mark(item, term_norm))

    matches.sort(key=lambda m: (m.get("active"), m.get("similarity", 0.0)), reverse=True)
    return matches[:limit]


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


def fetch_mark_details_by_ids(con: sqlite3.Connection, ids: list[int]) -> dict[int, sqlite3.Row]:
    if not ids:
        return {}
    placeholders = ",".join(["?"] * len(ids))
    rows = con.execute(
        "SELECT * FROM marks WHERE id IN (" + placeholders + ")",
        tuple(ids),
    ).fetchall()
    return {int(row["id"]): row for row in rows}


def score_match_conflict(match: dict[str, Any], term_norm: str, reference_classes: list[str]) -> float:
    mark_norm = norm_text(match.get("mark_text", ""))
    sim = float(match.get("similarity", 0.0))
    typo_sim = typo_similarity(term_norm, mark_norm)
    exact = mark_norm == term_norm
    close_phrase = is_close_phrase_match(term_norm, mark_norm, sim)
    same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
    active = bool(match.get("active"))

    score = 0.0
    if exact:
        score += 7.0
    elif same_class and sim >= 0.97:
        score += 5.5
    elif same_class and close_phrase:
        score += 4.0
    elif typo_sim >= 0.86:
        score += 2.0
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
    strongest_typo_like = 0

    for match in matches[:10]:
        mark_norm = norm_text(match.get("mark_text", ""))
        sim = float(match.get("similarity", 0.0))
        typo_sim = typo_similarity(term_norm, mark_norm)
        exact = mark_norm == term_norm
        same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
        active = bool(match.get("active"))
        score = score_match_conflict(match, term_norm, reference_classes)
        scored.append((score, match, exact, same_class))
        if typo_sim >= 0.82:
            strongest_typo_like += 1

        if active and same_class and (exact or sim >= 0.94):
            active_same_class_strong += 1
        elif active and not same_class and sim >= 0.94:
            active_cross_class_strong += 1
        elif score >= 5.0:
            moderate_matches += 1

    top_score, top_match, top_exact, top_same_class = max(scored, key=lambda item: item[0])
    total_score = sum(score for score, _, _, _ in scored)
    top_active = bool(top_match.get("active"))
    top_typo_sim = typo_similarity(term_norm, norm_text(top_match.get("mark_text", "")))

    if top_exact and top_same_class and top_active:
        return "high", "Identical active mark found in the same class"
    if active_same_class_strong > 0:
        return "high", "Very similar active mark found in the same class"
    if active_same_class_strong + moderate_matches >= 2 and total_score >= 15:
        return "high", "Multiple similar active marks found"

    if active_cross_class_strong > 0 and total_score >= 9:
        return "medium", "Active similar marks found in other classes"
    if top_typo_sim >= 0.82 and not top_same_class:
        return "low", "Only typo-like or broad similar marks found"
    if strongest_typo_like > 0 and moderate_matches == 0 and total_score < 9:
        return "low", "Only typo-like or broad similar marks found"
    if moderate_matches >= 2 or total_score >= 10:
        return "medium", "Multiple similar active marks found"
    if top_score >= 5.5 and top_active and (top_same_class or top_typo_sim < 0.82):
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
        "_id": int(row["id"]) if "id" in row.keys() and row["id"] is not None else None,
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


def hydrate_visible_matches(con: sqlite3.Connection, matches: list[dict[str, Any]], term_norm: str) -> list[dict[str, Any]]:
    ids = [int(match["_id"]) for match in matches if isinstance(match.get("_id"), int)]
    if not ids:
        return [{k: v for k, v in match.items() if not k.startswith("_")} for match in matches]

    detailed_rows = fetch_mark_details_by_ids(con, ids)
    hydrated: list[dict[str, Any]] = []
    for match in matches:
        row_id = match.get("_id")
        detailed = detailed_rows.get(int(row_id)) if isinstance(row_id, int) else None
        if detailed is not None:
            merged = summarize_mark(detailed, term_norm)
            merged["similarity"] = match.get("similarity", merged.get("similarity", 0.0))
            merged["data_source"] = match.get("data_source", merged.get("data_source", "local_database"))
            hydrated.append({k: v for k, v in merged.items() if not k.startswith("_")})
        else:
            hydrated.append({k: v for k, v in match.items() if not k.startswith("_")})
    return hydrated


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
    typo_sim = typo_similarity(term_norm, mark_norm)
    exact = mark_norm == term_norm
    close_phrase = is_close_phrase_match(term_norm, mark_norm, sim)
    same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
    overlap = token_overlap_ratio(term_norm, mark_norm)
    term_tokens = tokenize(term_norm)
    mark_tokens = tokenize(mark_norm)
    phrase_like = len(term_tokens) > 1 and len(mark_tokens) > 1
    leading_token_phrase = phrase_like and mark_tokens[0] == term_tokens[0]
    return (
        1 if exact else 0,
        1 if leading_token_phrase else 0,
        1 if phrase_like else 0,
        1 if typo_sim >= 0.82 else 0,
        1 if close_phrase else 0,
        1 if same_class else 0,
        1 if match.get("active") else 0,
        overlap,
        sim,
        typo_sim,
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


def shortlist_relevant_matches(
    matches: list[dict[str, Any]],
    term_norm: str,
    reference_classes: list[str],
    max_results: int = 10,
) -> list[dict[str, Any]]:
    ranked = prioritize_exact_matches(matches, term_norm, reference_classes)
    target_results = min(max_results, 10)
    minimum_results = min(5, len(ranked))

    def relevance_key(match: dict[str, Any]) -> tuple:
        mark_norm = norm_text(match.get("mark_text", ""))
        sim = float(match.get("similarity", 0.0))
        overlap = token_overlap_ratio(term_norm, mark_norm)
        same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
        prefix = mark_norm.startswith(term_norm) or term_norm.startswith(mark_norm)
        shared_token = overlap > 0
        close_phrase = is_close_phrase_match(term_norm, mark_norm, sim)
        term_tokens = tokenize(term_norm)
        mark_tokens = tokenize(mark_norm)
        leading_token_phrase = len(term_tokens) > 1 and len(mark_tokens) > 1 and mark_tokens[0] == term_tokens[0]
        weak_penalty = 1 if (sim < 0.45 and overlap == 0 and not prefix) else 0
        return (
            1 if same_class else 0,
            1 if leading_token_phrase else 0,
            1 if shared_token else 0,
            1 if prefix else 0,
            1 if close_phrase else 0,
            1 if match.get("active") else 0,
            sim,
            overlap,
            -weak_penalty,
        )

    reranked = sorted(ranked, key=relevance_key, reverse=True)
    shortlisted: list[dict[str, Any]] = []
    term_tokens = tokenize(term_norm)
    leading_token = term_tokens[0] if term_tokens else ""

    for match in reranked:
        mark_norm = norm_text(match.get("mark_text", ""))
        sim = float(match.get("similarity", 0.0))
        overlap = token_overlap_ratio(term_norm, mark_norm)
        same_class = bool(reference_classes and (set(match.get("class_codes", [])) & set(reference_classes)))
        prefix = mark_norm.startswith(term_norm) or term_norm.startswith(mark_norm)
        shared_token = overlap > 0
        keep = same_class or shared_token or prefix or sim >= 0.55
        if keep or len(shortlisted) < minimum_results:
            shortlisted.append(match)
        if len(shortlisted) >= target_results:
            break

    if leading_token and len(term_tokens) > 1 and len(shortlisted) < target_results:
        seen_marks = {match.get("reg_no") or match.get("mark_text") for match in shortlisted}
        for match in reranked:
            mark_norm = norm_text(match.get("mark_text", ""))
            mark_tokens = tokenize(mark_norm)
            key = match.get("reg_no") or match.get("mark_text")
            if key in seen_marks:
                continue
            if len(mark_tokens) > 1 and mark_tokens[0] == leading_token:
                shortlisted.append(match)
                seen_marks.add(key)
            if len(shortlisted) >= target_results:
                break

    return shortlisted or reranked[:minimum_results]


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
    request_started = perf_counter()
    ok, msg = ensure_index()
    if not ok:
        return jsonify({"error": msg}), 400

    payload = request.get_json(force=True) or {}
    term = (payload.get("trademark") or "").strip()
    country = (payload.get("country") or "").strip()
    class_filter = parse_classes(payload.get("classes") or "")
    include_patents = bool(payload.get("include_patents", True))
    similar_limit = parse_pagination_value(payload.get("limit"), DEFAULT_SIMILAR_LIMIT, minimum=1, maximum=MAX_SIMILAR_LIMIT)
    similar_offset = parse_pagination_value(payload.get("offset"), 0, minimum=0, maximum=1000)

    if not term:
        return jsonify({"error": "Missing trademark"}), 400
    if not country:
        return jsonify({"error": "Missing country"}), 400
    if len(term.strip()) < 3:
        return jsonify({"error": "Please enter at least 3 characters."}), 400

    term_norm = normalize_text(term)
    con = open_db()
    if not country_available(con, country) and not fallback_allowed(country):
        con.close()
        return jsonify(
            {
                "error": "No records found for this country in the current index.",
                "country": country,
            }
        ), 400
    stage_timings = {
        "exact_sql_ms": 0.0,
        "punctuation_exact_ms": 0.0,
        "punctuation_prefix_ms": 0.0,
        "fts_ms": 0.0,
        "python_scoring_ms": 0.0,
        "supplemental_lookup_ms": 0.0,
        "total_request_ms": 0.0,
    }

    exact_rows, exact_timings = query_exact_candidates(con, term, term_norm, country)
    for key, value in exact_timings.items():
        stage_timings[key] += value

    result_source = "local_database"
    fallback_used = False
    fallback_error = ""
    warnings: list[str] = []
    rows: list[sqlite3.Row] = []

    supplemental_matches: list[dict[str, Any]] = []
    supplemental_start = perf_counter()
    if not exact_rows:
        supplemental_matches = query_supplemental_candidates(term_norm, country, exact_only=True)
    stage_timings["supplemental_lookup_ms"] += (perf_counter() - supplemental_start) * 1000

    if exact_rows:
        rows = exact_rows
        if not needs_runtime_normalized_search(term) and len(exact_rows) < MAX_RETURNED_MATCHES:
            related_rows, related_ms = query_related_prefix_candidates(
                con,
                term_norm,
                country,
                limit=MAX_RETURNED_MATCHES,
            )
            stage_timings["punctuation_prefix_ms"] += related_ms
            seen_ids = {row["id"] for row in rows}
            for row in related_rows:
                if row["id"] in seen_ids:
                    continue
                rows.append(row)
                seen_ids.add(row["id"])
                if len(rows) >= MAX_RETURNED_MATCHES:
                    break
    elif supplemental_matches:
        result_source = "supplemental_source"
    else:
        rows, query_timings = query_candidates(con, term, term_norm, country, skip_exact_search=True)
        for key, value in query_timings.items():
            stage_timings[key] += value

    if not rows and not supplemental_matches and fallback_allowed(country):
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

    rows = dedupe_mark_rows(rows)
    matches = [summarize_mark(r, term_norm) for r in rows]
    for match in matches:
        match["data_source"] = result_source if result_source != "local_database" else "local_database"

    if not has_exact_or_strong_result(matches, term_norm):
        if not supplemental_matches:
            supplemental_start = perf_counter()
            supplemental_matches = query_supplemental_candidates(term_norm, country)
            stage_timings["supplemental_lookup_ms"] += (perf_counter() - supplemental_start) * 1000
        if supplemental_matches:
            seen_reg_nos = {m.get("reg_no", "") for m in matches}
            for supplemental_match in supplemental_matches:
                reg_no = supplemental_match.get("reg_no", "")
                if reg_no in seen_reg_nos:
                    continue
                matches.append(supplemental_match)
                seen_reg_nos.add(reg_no)
            if not rows:
                result_source = "supplemental_source"

    stage_timings["total_request_ms"] = (perf_counter() - request_started) * 1000
    app.logger.info(
        "check timing trademark=%r punctuation_fast=%s exact_sql=%.1fms punctuation_exact=%.1fms punctuation_prefix=%.1fms fts=%.1fms python_scoring=%.1fms supplemental=%.1fms total=%.1fms",
        term,
        needs_runtime_normalized_search(term),
        stage_timings["exact_sql_ms"],
        stage_timings["punctuation_exact_ms"],
        stage_timings["punctuation_prefix_ms"],
        stage_timings["fts_ms"],
        stage_timings["python_scoring_ms"],
        stage_timings["supplemental_lookup_ms"],
        stage_timings["total_request_ms"],
    )

    reference_classes = determine_reference_classes(matches, class_filter, term_norm)
    matches = prioritize_exact_matches(matches, term_norm, reference_classes)
    if has_exact_result(matches, term_norm):
        matches = matches[:MAX_RETURNED_MATCHES]
    else:
        matches = shortlist_relevant_matches(
            matches,
            term_norm,
            reference_classes,
            max_results=MAX_RETURNED_MATCHES,
        )
    chosen_class_matches, cross_class_matches = split_mark_groups(matches, reference_classes)
    chosen_class_matches = prioritize_exact_matches(chosen_class_matches, term_norm, reference_classes)
    cross_class_matches = prioritize_exact_matches(cross_class_matches, term_norm, reference_classes)
    all_matches = chosen_class_matches + cross_class_matches
    total_similar_count = len(all_matches)
    page_matches = all_matches[similar_offset:similar_offset + similar_limit]
    page_matches = hydrate_visible_matches(con, page_matches, term_norm)
    page_chosen_class_matches, page_cross_class_matches = split_mark_groups(page_matches, reference_classes)

    risk, risk_explanation = score_risk(all_matches, reference_classes, term_norm)

    patents = [summarize_patent(r, term_norm) for r in patent_rows]
    con.close()
    patents.sort(key=lambda p: (p["active"], p["similarity"]), reverse=True)
    patents = patents[:50]

    ukipo_manual_search_url = "https://trademarks.ipo.gov.uk/ipo-tmtext?reset"

    if not all_matches:
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
            "match_count": total_similar_count,
            "total_similar_count": total_similar_count,
            "returned_count": len(page_matches),
            "has_more": (similar_offset + len(page_matches)) < total_similar_count,
            "next_offset": similar_offset + len(page_matches),
            "patent_count": len(patents),
            "notes": [
                "Usage is inferred from status/expiry fields in the dataset; it is not verified market use.",
                "Owner business type is not provided by the dataset; owner_type is inferred from the owner name.",
            ],
            "similar_marks": page_matches,
            "chosen_class_matches": page_chosen_class_matches,
            "cross_class_matches": page_cross_class_matches,
            "patents": patents,
        }
    )


@app.route("/warmup")
def warmup():
    try:
        elapsed_ms = run_lightweight_warmup()
        return jsonify({"ok": True, "warmed": True, "duration_ms": round(elapsed_ms, 1)})
    except Exception as exc:
        return jsonify({"ok": False, "warmed": False, "message": str(exc)}), 503


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


start_background_warmup()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
