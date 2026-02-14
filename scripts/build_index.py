import csv
import re
import sqlite3
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from pathlib import Path

DB_PATH = Path("data/trademarks.sqlite")

HEADER_MAP = {
    "Trade Mark": "reg_no",
    "Mark Text": "mark_text",
    "Name": "owner_name",
    "Country": "country",
    "Status": "status",
    "Category of Mark": "category",
    "Mark Type": "mark_type",
    "Filed": "filed",
    "Published": "published",
    "Registered": "registered",
    "Expired": "expired",
    "Renewal Due Date": "renewal_due",
}

CLASS_PREFIX = "Class"
NULL_VALUES = {"", "NULL", "null", "N/A", "n/a"}


def norm_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_date(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Expect YYYY-MM-DD, but keep original if not parseable
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        return s


def excel_date_to_iso(s: str) -> str:
    s = (s or "").strip()
    if not s or s in NULL_VALUES:
        return ""
    try:
        val = float(s)
        if val <= 0:
            return ""
        base = date(1899, 12, 30)
        d = base + timedelta(days=int(val))
        return d.isoformat()
    except ValueError:
        return s


def discover_txt_files(root: Path) -> list[Path]:
    files = []
    for path in root.rglob("*.txt"):
        if path.is_file() and is_trademark_text_file(path):
            files.append(path)
    return files


def is_trademark_text_file(path: Path) -> bool:
    try:
        raw = path.read_bytes()[:4096]
    except Exception:
        return False

    # UTF-16 LE/BE header variants and UTF-8 header variant
    if b"T\x00r\x00a\x00d\x00e\x00 \x00M\x00a\x00r\x00k\x00" in raw:
        return True
    if b"\x00T\x00r\x00a\x00d\x00e\x00 \x00M\x00a\x00r\x00k" in raw:
        return True
    if b"Trade Mark|" in raw:
        return True
    return False


def discover_xlsx_files(root: Path) -> list[Path]:
    files = []
    for path in root.rglob("*.xlsx"):
        if path.is_file():
            files.append(path)
    return files


def discover_xml_files(root: Path) -> list[Path]:
    files = []
    for path in root.rglob("*.xml"):
        if path.is_file():
            files.append(path)
    return files


def connect_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    return con


def setup_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS marks (
            id INTEGER PRIMARY KEY,
            reg_no TEXT UNIQUE,
            mark_text TEXT,
            mark_text_norm TEXT,
            owner_name TEXT,
            owner_type TEXT,
            country TEXT,
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
            source_file TEXT
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_marks_country ON marks(country)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_marks_status ON marks(status)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_marks_mark_norm ON marks(mark_text_norm)")
    con.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS marks_fts
        USING fts5(mark_text, owner_name, content='marks', content_rowid='id')
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS patents (
            id INTEGER PRIMARY KEY,
            application_number TEXT,
            publication_number TEXT,
            ipsum TEXT,
            earliest_filing_date TEXT,
            filing_date TEXT,
            lodged_date TEXT,
            publication_a_date TEXT,
            publication_b_date TEXT,
            applicant_name TEXT,
            applicant_country_code TEXT,
            applicant_postcode TEXT,
            applicant_county TEXT,
            applicant_region TEXT,
            applicant_country TEXT,
            ipc7 TEXT,
            ipc8 TEXT,
            pct_filing_date TEXT,
            pct_publication_date TEXT,
            last_renewal_date TEXT,
            last_annuity_year TEXT,
            date_not_in_force TEXT,
            reason_not_in_force TEXT,
            status TEXT,
            source_file TEXT
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_patents_status ON patents(status)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_patents_applicant ON patents(applicant_name)")
    con.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS patents_fts
        USING fts5(application_number, publication_number, applicant_name, ipc7, ipc8, content='patents', content_rowid='id')
        """
    )


def infer_owner_type(name: str) -> str:
    if not name:
        return "unknown"
    n = name.lower()
    if any(x in n for x in [" ltd", " limited", " llc", " inc", " corp", " gmbh", " plc", " llp"]):
        return "company"
    if " and " in n or " & " in n:
        return "company"
    return "individual_or_other"


def build_class_codes(row: dict) -> str:
    classes = []
    for k, v in row.items():
        if k.startswith(CLASS_PREFIX):
            try:
                class_num = int(k[len(CLASS_PREFIX):])
            except ValueError:
                continue
            if (v or "").strip() not in ("", "0", "No", "N", "False"):
                classes.append(str(class_num))
    return ",".join(classes)


def read_rows(path: Path):
    # UK IPO exports are pipe-delimited and usually UTF-16.
    # Some files are UTF-16 without BOM, so we try multiple encodings.
    last_err = None
    for enc in ("utf-16", "utf-16-le", "utf-16-be", "utf-8-sig"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f, delimiter="|")
                headers = next(reader)
                headers = [h.replace("\ufeff", "").strip() for h in headers]
                if "Trade Mark" not in headers or "Mark Text" not in headers:
                    raise ValueError("missing expected trademark headers")
                yield headers
                for row in reader:
                    if not row:
                        continue
                    yield row
                return
        except Exception as exc:
            last_err = exc
            continue

    raise ValueError(f"Unsupported or invalid trademark text file: {path} ({last_err})")


def col_to_index(col: str) -> int:
    idx = 0
    for ch in col:
        if "A" <= ch <= "Z":
            idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def read_xlsx_rows(path: Path):
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(path) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            for event, elem in ET.iterparse(z.open("xl/sharedStrings.xml")):
                if elem.tag == ns + "si":
                    texts = [t.text or "" for t in elem.findall(".//" + ns + "t")]
                    shared.append("".join(texts))
                    elem.clear()

        sheet = "xl/worksheets/sheet1.xml"
        headers = None
        for event, elem in ET.iterparse(z.open(sheet)):
            if elem.tag != ns + "row":
                continue
            cells = elem.findall(ns + "c")
            row = {}
            for c in cells:
                ref = c.get("r") or ""
                col_letters = "".join([ch for ch in ref if ch.isalpha()])
                if not col_letters:
                    continue
                idx = col_to_index(col_letters)
                v = c.find(ns + "v")
                if v is None:
                    val = ""
                else:
                    val = v.text or ""
                    if c.get("t") == "s":
                        try:
                            val = shared[int(val)]
                        except Exception:
                            val = ""
                row[idx] = val

            if headers is None:
                max_idx = max(row.keys()) if row else -1
                headers = [row.get(i, "") for i in range(max_idx + 1)]
                yield headers
            else:
                max_idx = max(row.keys()) if row else -1
                values = [row.get(i, "") for i in range(max_idx + 1)]
                yield values

            elem.clear()


def ingest_file(con: sqlite3.Connection, path: Path) -> None:
    headers = None
    batch = []
    total = 0

    for row in read_rows(path):
        if headers is None:
            headers = row
            continue
        # Pad or trim
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[: len(headers)]
        data = dict(zip(headers, row))

        reg_no = (data.get("Trade Mark") or "").strip()
        mark_text = (data.get("Mark Text") or "").strip()
        owner_name = (data.get("Name") or "").strip()
        country = (data.get("Country") or "").strip()
        status = (data.get("Status") or "").strip()
        category = (data.get("Category of Mark") or "").strip()
        mark_type = (data.get("Mark Type") or "").strip()

        filed = parse_date(data.get("Filed") or "")
        published = parse_date(data.get("Published") or "")
        registered = parse_date(data.get("Registered") or "")
        expired = parse_date(data.get("Expired") or "")
        renewal_due = parse_date(data.get("Renewal Due Date") or "")

        mark_text_norm = norm_text(mark_text)
        owner_type = infer_owner_type(owner_name)
        class_codes = build_class_codes(data)

        batch.append(
            (
                reg_no,
                mark_text,
                mark_text_norm,
                owner_name,
                owner_type,
                country,
                status,
                category,
                mark_type,
                filed,
                published,
                registered,
                expired,
                renewal_due,
                class_codes,
                "",
                str(path),
            )
        )

        if len(batch) >= 5000:
            insert_batch(con, batch)
            total += len(batch)
            batch.clear()

    if batch:
        insert_batch(con, batch)
        total += len(batch)

    print(f"Ingested {total} rows from {path}")


def clean_cell(v: str) -> str:
    v = (v or "").strip()
    if v in NULL_VALUES:
        return ""
    return v


def ingest_patents(con: sqlite3.Connection, path: Path) -> None:
    headers = None
    batch = []
    total = 0

    for row in read_xlsx_rows(path):
        if headers is None:
            headers = row
            continue
        if not row:
            continue

        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        data = dict(zip(headers, row))

        def get(key: str) -> str:
            return clean_cell(data.get(key, ""))

        batch.append(
            (
                get("Application number"),
                get("Publication number"),
                get("IPSUM"),
                excel_date_to_iso(get("Earliest filing date")),
                excel_date_to_iso(get("Filing date")),
                excel_date_to_iso(get("Lodged date")),
                excel_date_to_iso(get("A publication date")),
                excel_date_to_iso(get("B publication date")),
                get("Applicant name"),
                get("Applicant Country code"),
                get("Applicant postcode"),
                get("Applicant county"),
                get("Applicant region"),
                get("Applicant country"),
                get("IPC7"),
                get("IPC8"),
                excel_date_to_iso(get("PCT filing date")),
                excel_date_to_iso(get("PCT publication date")),
                excel_date_to_iso(get("Last renewal date")),
                get("Last annuity year"),
                excel_date_to_iso(get("Date not in force")),
                get("Reason not in force"),
                get("Status"),
                str(path),
            )
        )

        if len(batch) >= 5000:
            con.executemany(
                """
                INSERT INTO patents(
                    application_number, publication_number, ipsum,
                    earliest_filing_date, filing_date, lodged_date,
                    publication_a_date, publication_b_date,
                    applicant_name, applicant_country_code, applicant_postcode,
                    applicant_county, applicant_region, applicant_country,
                    ipc7, ipc8, pct_filing_date, pct_publication_date,
                    last_renewal_date, last_annuity_year, date_not_in_force,
                    reason_not_in_force, status, source_file
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                batch,
            )
            total += len(batch)
            batch.clear()

    if batch:
        con.executemany(
            """
            INSERT INTO patents(
                application_number, publication_number, ipsum,
                earliest_filing_date, filing_date, lodged_date,
                publication_a_date, publication_b_date,
                applicant_name, applicant_country_code, applicant_postcode,
                applicant_county, applicant_region, applicant_country,
                ipc7, ipc8, pct_filing_date, pct_publication_date,
                last_renewal_date, last_annuity_year, date_not_in_force,
                reason_not_in_force, status, source_file
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            batch,
        )
        total += len(batch)

    print(f"Ingested {total} patent rows from {path}")


def normalize_country_from_office(reg_no: str, office_code: str) -> str:
    reg_no = (reg_no or "").upper()
    office_code = (office_code or "").upper()
    if reg_no.startswith("UK") or office_code in {"GB", "UK"}:
        return "United Kingdom"
    if reg_no.startswith("US") or office_code == "US":
        return "United States"
    if office_code in {"EM", "EU", "EP"} or reg_no.startswith("EU"):
        return "European Union"
    if reg_no.startswith("WO") or office_code:
        return "Rest of World"
    return "Rest of World"


def ingest_journal_xml(con: sqlite3.Connection, path: Path) -> None:
    batch = []
    total = 0

    for event, elem in ET.iterparse(path, events=("end",)):
        if elem.tag != "TradeMark":
            continue

        def text_at(tag: str) -> str:
            child = elem.find(tag)
            return (child.text or "").strip() if child is not None else ""

        reg_no = text_at("RegistrationNumber")
        app_date = text_at("ApplicationDate")
        office_code = text_at("RegistrationOfficeCode")
        mark_type = text_at("MarkFeature")
        kind_mark = text_at("KindMark")

        mark_text = ""
        wm = elem.find("./WordMarkSpecification/MarkVerbalElementText")
        if wm is not None and wm.text:
            mark_text = wm.text.strip()

        applicant = ""
        an = elem.find("./ApplicantDetails/ApplicantName")
        if an is not None and an.text:
            applicant = an.text.strip()

        class_nums = []
        goods = []
        for g in elem.findall("./GoodsServicesDetails"):
            cn = g.find("ClassNumber")
            desc = g.find("GoodsServicesDescription")
            if cn is not None and cn.text:
                class_nums.append(cn.text.strip())
            if desc is not None and desc.text:
                goods.append(desc.text.strip())

        class_codes = ",".join([c for c in class_nums if c])
        goods_services = " | ".join(goods)[:4000]

        mark_text_norm = norm_text(mark_text)
        owner_type = infer_owner_type(applicant)
        country = normalize_country_from_office(reg_no, office_code)

        batch.append(
            (
                reg_no,
                mark_text,
                mark_text_norm,
                applicant,
                owner_type,
                country,
                "Published",
                "",
                mark_type or kind_mark,
                parse_date(app_date),
                parse_date(app_date),
                "",
                "",
                "",
                class_codes,
                goods_services,
                str(path),
            )
        )

        if len(batch) >= 5000:
            insert_batch(con, batch)
            total += len(batch)
            batch.clear()

        elem.clear()

    if batch:
        insert_batch(con, batch)
        total += len(batch)

    print(f"Ingested {total} journal rows from {path}")


def insert_batch(con: sqlite3.Connection, batch: list[tuple]) -> None:
    con.executemany(
        """
        INSERT OR IGNORE INTO marks(
            reg_no, mark_text, mark_text_norm, owner_name, owner_type,
            country, status, category, mark_type,
            filed, published, registered, expired, renewal_due,
            class_codes, goods_services, source_file
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?)
        """,
        batch,
    )
    # Insert into FTS using last inserted rowids
    # We can refresh FTS content from marks after inserts for simplicity


def rebuild_fts(con: sqlite3.Connection) -> None:
    # Use FTS5 rebuild command for external content table
    con.execute("INSERT INTO marks_fts(marks_fts) VALUES('rebuild')")
    con.execute("INSERT INTO patents_fts(patents_fts) VALUES('rebuild')")


def main() -> None:
    root = Path(".")
    files = discover_txt_files(root)
    xlsx_files = discover_xlsx_files(root)
    xml_files = discover_xml_files(root)
    if not files and not xlsx_files and not xml_files:
        print("No data files found for ingestion.")
        return

    if DB_PATH.exists():
        DB_PATH.unlink()

    con = connect_db()
    setup_schema(con)

    for path in files:
        ingest_file(con, path)

    for path in xlsx_files:
        ingest_patents(con, path)

    for path in xml_files:
        ingest_journal_xml(con, path)

    rebuild_fts(con)
    con.commit()
    con.close()
    print(f"Index built at {DB_PATH}")


if __name__ == "__main__":
    main()
