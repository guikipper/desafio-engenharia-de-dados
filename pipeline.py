"""
Full pipeline: extracts data from the SNCR website and loads it into PostgreSQL.

1. Downloads CSVs by state+municipality (with municipality filter) — parallel
2. Downloads CSVs by state only (without municipality, "All") — parallel
3. Merges both sets (they contain complementary data)
4. Looks up details for each INCRA code — parallel
5. Loads everything into PostgreSQL with idempotency (UPSERT)
"""

import csv
import io
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests

# ── Configuration ────────────────────────────────────────────────────────────

BASE_URL = os.getenv("SNCR_BASE_URL", "https://data-engineer-challenge-production.up.railway.app")
DB_DSN = os.getenv("DATABASE_URL", "postgresql://sncr:sncr@localhost:5432/sncr")

# MAX_RETRIES: how many times to retry a failed HTTP request before giving up.
# INITIAL_BACKOFF: wait time (in seconds) before the first retry.
#   Doubles each attempt: 1s, 2s, 4s, 8s, 16s (exponential backoff).
#   This avoids hammering the server when it's having issues.
MAX_RETRIES = 5
INITIAL_BACKOFF = 1

WORKERS_CSV = 10      # parallel threads for CSV downloads
WORKERS_INCRA = 10    # parallel threads for INCRA lookups

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# ── HTTP Helpers ─────────────────────────────────────────────────────────────

# session_local is a thread-local storage object.
# Each thread gets its own independent copy of any attribute set on it.
# We use it to give each thread its own requests.Session, so they don't
# share connections and step on each other.
session_local = threading.local()


def get_session() -> requests.Session:
    """
    Returns a requests.Session for the current thread.
    A Session reuses the underlying TCP connection across multiple requests
    to the same server, which is faster than opening a new connection each time.
    Each thread needs its own Session because Sessions are not thread-safe.
    """
    if not hasattr(session_local, "session"):
        session_local.session = requests.Session()
    return session_local.session


def get_json(endpoint: str) -> dict | list:
    """
    Makes a GET request to the API and returns the parsed JSON response.
    Retries up to MAX_RETRIES times with exponential backoff on failure.
    Used for simple endpoints like /api/estados and /api/municipios/{uf}.
    """
    s = get_session()
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(f"{BASE_URL}{endpoint}", timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = INITIAL_BACKOFF * (2 ** attempt)
                log.warning(f"Retry {attempt+1} for {endpoint}: {e}")
                time.sleep(wait)
            else:
                raise


def get_captcha() -> tuple[str, str]:
    """
    Requests a new captcha from /api/captcha.
    The API returns: { captcha_id: "abc", captcha_image: "1 2 3 4 5", digits: "12345" }
    The 'digits' field IS the answer — the server gives it away in the response.
    Returns (captcha_id, digits) to be sent back when downloading data.
    Each captcha is single-use: once used, it's invalidated.
    """
    data = get_json("/api/captcha")
    return data["captcha_id"], data["digits"]


def download_csv(uf: str, municipality: str | None = None) -> str | None:
    """
    Downloads a CSV file from the export endpoint.
    Requires a fresh captcha for each download (single-use).

    Args:
        uf: State code (e.g., "SP", "AC")
        municipality: Municipality name, or None for "All municipalities"

    Returns the CSV text content, or None if all retries failed.
    """
    s = get_session()
    for attempt in range(MAX_RETRIES):
        try:
            captcha_id, digits = get_captcha()
            params = {"uf": uf, "captcha_id": captcha_id, "captcha_value": digits}
            if municipality:
                params["municipio"] = municipality

            r = s.get(f"{BASE_URL}/api/dados-abertos/exportar", params=params, timeout=60)

            if r.status_code == 200 and "text/csv" in r.headers.get("content-type", ""):
                return r.text

            detail = ""
            try:
                detail = r.json().get("detail", "")
            except Exception:
                detail = r.text[:200]
            raise Exception(f"HTTP {r.status_code}: {detail}")

        except Exception as e:
            label = f"{uf}/{municipality}" if municipality else f"{uf}/all"
            if attempt < MAX_RETRIES - 1:
                wait = INITIAL_BACKOFF * (2 ** attempt)
                log.warning(f"Retry {attempt+1} for {label}: {e}")
                time.sleep(wait)
            else:
                log.error(f"Final failure for {label}: {e}")
                return None


def lookup_property(incra_code: str) -> dict | None:
    """
    Looks up a property by INCRA code via /api/consulta/imovel/{code}.
    Returns the full property details (area, status, owners list),
    or None if all retries failed.
    Requires a fresh captcha for each lookup.
    """
    s = get_session()
    for attempt in range(MAX_RETRIES):
        try:
            captcha_id, digits = get_captcha()
            r = s.get(
                f"{BASE_URL}/api/consulta/imovel/{incra_code}",
                params={"captcha_id": captcha_id, "captcha_value": digits},
                timeout=30,
            )
            if r.status_code == 200:
                return r.json()
            raise Exception(f"HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(INITIAL_BACKOFF * (2 ** attempt))
            else:
                log.error(f"Failed INCRA lookup {incra_code}: {e}")
                return None


def parse_csv(text: str) -> list[dict]:
    """
    Parses a CSV string (semicolon-delimited) into a list of dicts.
    Each dict has keys from the CSV header: codigo_incra, matricula, municipio, etc.
    """
    return list(csv.DictReader(io.StringIO(text), delimiter=";"))


# ── Database ─────────────────────────────────────────────────────────────────

def get_db():
    """Opens a connection to PostgreSQL. Used for all database operations."""
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    return conn



def upsert_property(cur, row: dict):
    """
    Inserts or updates a property in the 'properties' table.
    Uses ON CONFLICT (UPSERT): if the incra_code already exists, it updates
    the existing row instead of failing. This makes the pipeline idempotent —
    you can run it multiple times without creating duplicates.
    """
    cur.execute(
        """INSERT INTO properties (incra_code, registration, municipality, state, name, acquisition_pct)
           VALUES (%(codigo_incra)s, %(matricula)s, %(municipio)s, %(uf)s, %(denominacao)s, %(pct_obtencao)s)
           ON CONFLICT (incra_code) DO UPDATE SET
               registration = EXCLUDED.registration,
               municipality = EXCLUDED.municipality,
               state = EXCLUDED.state,
               name = EXCLUDED.name,
               acquisition_pct = EXCLUDED.acquisition_pct""",
        row,
    )


def upsert_property_details(cur, data: dict):
    """
    Updates a property with details from the INCRA lookup (area, status, name).
    This runs AFTER upsert_property, adding the fields that only come
    from the consultation endpoint (not from the CSV export).
    """
    cur.execute(
        """UPDATE properties SET
               area_hectares = %(area_hectares)s,
               status = %(situacao)s,
               name = %(denominacao)s
           WHERE incra_code = %(codigo_incra)s""",
        data,
    )


def upsert_owner(cur, incra_code: str, owner: dict):
    """
    Inserts or updates an owner in the 'owners' table.
    Uses ON CONFLICT on (incra_code, cpf) to avoid duplicates.
    One property can have multiple owners (1:N relationship).
    """
    cur.execute(
        """INSERT INTO owners (incra_code, owner_name, cpf, relationship, participation_pct)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (incra_code, cpf) DO UPDATE SET
               owner_name = EXCLUDED.owner_name,
               relationship = EXCLUDED.relationship,
               participation_pct = EXCLUDED.participation_pct""",
        (incra_code, owner["nome"], owner["cpf"], owner["vinculo"], owner["pct_participacao"]),
    )


# ── Parallel Workers ────────────────────────────────────────────────────────

def _worker_download_csv(args: tuple) -> tuple[str, str | None, str | None]:
    """
    Worker function executed by each thread in the CSV download pool.
    Receives (uf, municipality), downloads the CSV, and returns the result.
    The ThreadPoolExecutor calls this function in parallel across 10 threads.
    """
    uf, municipality = args
    label = f"{uf}/{municipality}" if municipality else f"{uf}/all"
    csv_text = download_csv(uf, municipality)
    return label, uf, csv_text


def _worker_lookup_incra(code: str) -> tuple[str, dict | None]:
    """
    Worker function executed by each thread in the INCRA lookup pool.
    Receives an INCRA code, looks it up, and returns the result.
    """
    data = lookup_property(code)
    return code, data


# ── Pipeline Steps ──────────────────────────────────────────────────────────

def step1_extract_csvs(conn) -> dict[str, dict]:
    """
    Step 1: Download all CSVs from the SNCR website.

    Downloads data in two ways (both are needed because they return different records):
    - WITH municipality: loops through each state's municipalities (205 downloads)
    - WITHOUT municipality: downloads all records per state at once (27 downloads)

    All 232 downloads run in parallel using 10 threads.
    Returns a dict of incra_code -> row with all unique records merged.
    """
    log.info("=" * 60)
    log.info("STEP 1: CSV Extraction")
    log.info("=" * 60)

    states = get_json("/api/estados")
    log.info(f"Found {len(states)} states.")

    all_records = {}

    # Build task list: WITH municipality
    tasks_with = []
    for state in states:
        uf = state["sigla"]
        municipalities = get_json(f"/api/municipios/{uf}")
        log.info(f"  {uf}: {len(municipalities)} municipalities")
        for mun in municipalities:
            tasks_with.append((uf, mun["nome"]))

    # Build task list: WITHOUT municipality
    tasks_without = [(state["sigla"], None) for state in states]

    all_tasks = tasks_with + tasks_without
    log.info(f"\nDownloading {len(tasks_with)} with municipality + {len(tasks_without)} without = {len(all_tasks)} total ({WORKERS_CSV} threads)")

    completed = 0
    errors = 0
    start = datetime.now()

    with ThreadPoolExecutor(max_workers=WORKERS_CSV) as executor:
        futures = {executor.submit(_worker_download_csv, t): t for t in all_tasks}

        for future in as_completed(futures):
            label, uf, csv_text = future.result()
            completed += 1

            if csv_text:
                rows = parse_csv(csv_text)
                for row in rows:
                    row["uf"] = uf
                    all_records[row["codigo_incra"]] = row

                if completed % 20 == 0:
                    log.info(f"  [{completed}/{len(all_tasks)}] {label}: {len(rows)} records | unique total: {len(all_records)}")
            else:
                errors += 1
                log.error(f"  [{completed}/{len(all_tasks)}] {label}: FAILED")

    duration = datetime.now() - start
    log.info(f"Extraction done in {duration}: {len(all_records)} unique INCRA codes, {errors} errors")
    return all_records


def step2_load_properties(conn, records: dict[str, dict]):
    """
    Step 2: Load properties into PostgreSQL.

    Takes the merged dict from step 1 and inserts each record into the
    'properties' table using UPSERT (insert or update if already exists).
    """
    log.info("=" * 60)
    log.info("STEP 2: Load properties into database")
    log.info("=" * 60)

    with conn.cursor() as cur:
        for i, (code, row) in enumerate(records.items(), 1):
            upsert_property(cur, row)
            if i % 500 == 0:
                log.info(f"  {i}/{len(records)} properties inserted...")
    conn.commit()
    log.info(f"  {len(records)} properties loaded.")


def step3_lookup_incra(conn, records: dict[str, dict]):
    """
    Step 3: Look up details and owners for each INCRA code.

    For each of the ~2989 properties, calls the INCRA consultation endpoint
    to get: area in hectares, status, and list of owners with CPF.
    Runs in parallel with 10 threads.
    Uses checkpointing: if interrupted, skips codes already in the database.
    """
    log.info("=" * 60)
    log.info("STEP 3: INCRA lookup (details + owners)")
    log.info("=" * 60)

    codes = sorted(records.keys())

    # Checkpointing: skip codes already looked up
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT incra_code FROM owners")
        already_done = {row[0] for row in cur.fetchall()}

    pending = [c for c in codes if c not in already_done]
    log.info(f"  {len(codes)} total, {len(already_done)} already done, {len(pending)} pending ({WORKERS_INCRA} threads)")

    if not pending:
        log.info("  Nothing to do.")
        return

    completed = 0
    errors = 0
    buffer = []
    start = datetime.now()

    with ThreadPoolExecutor(max_workers=WORKERS_INCRA) as executor:
        futures = {executor.submit(_worker_lookup_incra, c): c for c in pending}

        for future in as_completed(futures):
            code, data = future.result()
            completed += 1

            if data:
                buffer.append(data)
            else:
                errors += 1

            # Write to database in batches of 100
            if len(buffer) >= 100:
                with conn.cursor() as cur:
                    for d in buffer:
                        upsert_property_details(cur, d)
                        for owner in d.get("proprietarios", []):
                            upsert_owner(cur, d["codigo_incra"], owner)
                conn.commit()
                log.info(f"  [{completed}/{len(pending)}] checkpoint (batch of {len(buffer)}) | {errors} errors")
                buffer = []

    # Write remaining records
    if buffer:
        with conn.cursor() as cur:
            for d in buffer:
                upsert_property_details(cur, d)
                for owner in d.get("proprietarios", []):
                    upsert_owner(cur, d["codigo_incra"], owner)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM owners")
        total_owners = cur.fetchone()[0]

    duration = datetime.now() - start
    log.info(f"  Done in {duration}: {completed} looked up, {errors} errors, {total_owners} owners in database.")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("SNCR Pipeline started (parallel mode)")
    start = datetime.now()

    conn = get_db()

    records = step1_extract_csvs(conn)
    step2_load_properties(conn, records)
    step3_lookup_incra(conn, records)

    conn.close()

    duration = datetime.now() - start
    log.info("=" * 60)
    log.info(f"Pipeline completed in {duration}")
    log.info(f"Total: {len(records)} properties extracted and loaded.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
