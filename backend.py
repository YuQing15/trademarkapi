from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import threading
import urllib.request
from datetime import datetime, date
from pathlib import Path
from typing import Any
from difflib import SequenceMatcher

from flask import Flask, jsonify, request, send_from_directory, render_template
from flask_cors import CORS

DB_PATH = Path(os.getenv("TRADEMARK_DB_PATH", "data/trademarks.sqlite"))
DB_URL = os.getenv("TRADEMARK_DB_URL", "").strip()
_download_lock = threading.Lock()
_download_attempted = False

app = Flask(__name__)


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


def open_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA case_sensitive_like=ON")
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
    placeholders = ",".join(["?"] * len(countries))

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
    if rows:
        return rows

    # 2) Prefix match only (fast, index-friendly)
    if len(term_norm) < 4:
        return []
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
    if rows:
        return rows

    # No FTS fallback here to avoid long-running scans on large datasets.
    return []


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


def score_risk(matches: list[dict[str, Any]], class_filter: list[str]) -> str:
    active_strong = 0
    active_medium = 0
    for m in matches:
        if class_filter:
            if not set(class_filter) & set(m.get("class_codes", [])):
                continue
        if m["active"] and m["similarity"] >= 0.92:
            active_strong += 1
        elif m["active"] and m["similarity"] >= 0.85:
            active_medium += 1

    if active_strong > 0:
        return "high"
    if active_medium > 0:
        return "medium"
    return "low"


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
    sim = similarity(term_norm, mark_norm)

    filed = row["filed"] or ""
    registered = row["registered"] or ""
    expired = row["expired"] or ""

    return {
        "reg_no": row["reg_no"],
        "mark_text": mark_text,
        "owner_name": row["owner_name"],
        "owner_type": row["owner_type"],
        "country": row["country"],
        "status": row["status"],
        "category": row["category"],
        "mark_type": row["mark_type"],
        "filed": filed,
        "registered": registered,
        "expired": expired,
        "renewal_due": row["renewal_due"],
        "age_years": years_since(filed) if filed else None,
        "active": is_active(row["status"], expired),
        "class_codes": (row["class_codes"] or "").split(",") if row["class_codes"] else [],
        "goods_services": row["goods_services"] if "goods_services" in row.keys() else "",
        "similarity": round(sim, 4),
    }


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
    if not country_available(con, country):
        con.close()
        return jsonify(
            {
                "error": "No records found for this country in the current index.",
                "country": country,
            }
        ), 400
    rows = query_candidates(con, term_norm, country)
    patent_rows = query_patents(con, term_norm) if include_patents else []
    con.close()

    matches = [summarize_mark(r, term_norm) for r in rows]
    matches.sort(key=lambda m: (m["active"], m["similarity"]), reverse=True)

    # Keep top 50
    matches = matches[:50]

    risk = score_risk(matches, class_filter)

    patents = [summarize_patent(r, term_norm) for r in patent_rows]
    patents.sort(key=lambda p: (p["active"], p["similarity"]), reverse=True)
    patents = patents[:50]

    return jsonify(
        {
            "trademark": term,
            "country": country,
            "classes": class_filter,
            "risk_level": risk,
            "match_count": len(matches),
            "patent_count": len(patents),
            "notes": [
                "Usage is inferred from status/expiry fields in the dataset; it is not verified market use.",
                "Owner business type is not provided by the dataset; owner_type is inferred from the owner name.",
                "Patent dates/status reflect the dataset and do not confirm current legal enforceability.",
            ],
            "similar_marks": matches,
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
            "message": msg,
        }
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
