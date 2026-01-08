print(">>> TOP OF SCRIPT REACHED")

import os
from dotenv import load_dotenv
load_dotenv()

import datetime as dt
import traceback
import pandas as pd
from entsoe import EntsoePandasClient
import psycopg2
from psycopg2.extras import execute_values

# ---------------- CONFIG ----------------

ENTSOE_API_TOKEN = os.getenv("ENTSOE_API_TOKEN")
SOURCE_NAME = "ENTSOE"

PG_HOST    = os.getenv("PG_HOST")
PG_PORT    = os.getenv("PG_PORT", "5432")
PG_DB      = os.getenv("PG_DB")
PG_USER    = os.getenv("PG_USER")
PG_PASS    = os.getenv("PG_PASS")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

DEFAULT_FUEL_DETAIL = "Unknown"

ZONES = [
    # Core Europe
    ("DE_LU", "DE-LU"),
    ("FR", "FR"),
    ("NL", "NL"),
    ("BE", "BE"),
    ("AT", "AT"),
    ("CH", "CH"),
    ("PL", "PL"),
    ("CZ", "CZ"),
    ("SK", "SK"),
    ("HU", "HU"),
    ("SI", "SI"),
    ("HR", "HR"),

    # Iberia
    ("ES", "ES"),
    ("PT", "PT"),

    # Nordics
    ("NO_1", "NO1"),
    ("NO_2", "NO2"),
    ("NO_3", "NO3"),
    ("NO_4", "NO4"),
    ("NO_5", "NO5"),
    ("SE_1", "SE1"),
    ("SE_2", "SE2"),
    ("SE_3", "SE3"),
    ("SE_4", "SE4"),
    ("FI", "FI"),
    ("DK_1", "DK1"),
    ("DK_2", "DK2"),

    # Baltics
    ("EE", "EE"),
    ("LV", "LV"),
    ("LT", "LT"),

    # Italy (split zones)
    ("IT_NORD", "IT-NORD"),
    ("IT_CNOR", "IT-CNOR"),
    ("IT_CSUD", "IT-CSUD"),
    ("IT_SUD",  "IT-SUD"),
    ("IT_SICI", "IT-SICI"),
    ("IT_SARD", "IT-SARD"),

    # Balkans
    ("RO", "RO"),
    ("BG", "BG"),
    ("GR", "GR"),
    ("RS", "RS"),
    ("BA", "BA"),
    ("ME", "ME"),
    ("AL", "AL"),
    ("MK", "MK"),
]

# ------------- HELPERS ----------------

def get_client():
    if not ENTSOE_API_TOKEN:
        raise RuntimeError("ENTSOE_API_TOKEN is not set")
    return EntsoePandasClient(api_key=ENTSOE_API_TOKEN)


def normalize_fuel_detail(x) -> str:
    """
    Ensure fuel_detail is NEVER NULL/None and never an empty string.
    """
    if x is None:
        return DEFAULT_FUEL_DETAIL
    s = str(x).strip()
    return s if s else DEFAULT_FUEL_DETAIL


def normalize_fuel_type(x) -> str:
    """
    fuel_type should also never be None/empty (defensive).
    """
    if x is None:
        return "UnknownFuel"
    s = str(x).strip()
    return s if s else "UnknownFuel"


def fetch_generation_df(country_code: str,
                        bidding_zone: str,
                        start_utc: dt.datetime,
                        end_utc: dt.datetime) -> pd.DataFrame:
    print(f"[INFO] Fetching generation for {country_code} ({bidding_zone}) "
          f"from {start_utc} → {end_utc}")

    client = get_client()

    # ENTSO-E expects local market time for the zone.
    # You were using Europe/Berlin for everything; keep it consistent unless you want per-zone TZ logic.
    start = pd.Timestamp(start_utc).tz_convert("Europe/Berlin")
    end   = pd.Timestamp(end_utc).tz_convert("Europe/Berlin")

    df = client.query_generation(
        country_code=country_code,
        start=start,
        end=end,
        psr_type=None,
    )
    print(f"[INFO] Raw DataFrame shape for {bidding_zone}: {df.shape}")
    return df


def df_to_records(df: pd.DataFrame, bidding_zone: str):
    """
    Convert entsoe-py wide DataFrame into records:
        (time_utc, bidding_zone, fuel_type, fuel_detail, value_mw, source)
    """
    print(f"[INFO] Converting DataFrame to records for {bidding_zone}...")

    if df is None or df.empty:
        print(f"[WARN] No data returned from ENTSO-E for {bidding_zone}.")
        return []

    df = df.copy()

    # Ensure index is tz-aware UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize("Europe/Berlin").tz_convert("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    cols = list(df.columns)
    print(f"[INFO] {len(df)} timestamps, {len(cols)} PSR columns for {bidding_zone}")

    records = []
    for ts, row in df.iterrows():
        # Optional: if you want hour-aligned timestamps always:
        # ts = ts.floor("h")

        for psr in cols:
            val = row[psr]
            if pd.isna(val):
                continue

            if isinstance(psr, tuple):
                fuel_type_raw   = psr[0] if len(psr) >= 1 else None
                fuel_detail_raw = psr[1] if len(psr) >= 2 else None
            else:
                fuel_type_raw   = psr
                fuel_detail_raw = DEFAULT_FUEL_DETAIL  # IMPORTANT: not None

            fuel_type = normalize_fuel_type(fuel_type_raw)
            fuel_detail = normalize_fuel_detail(fuel_detail_raw)

            records.append(
                (
                    ts.to_pydatetime(),   # time_utc (tz-aware)
                    bidding_zone,         # bidding_zone
                    fuel_type,            # fuel_type (non-null)
                    fuel_detail,          # fuel_detail (non-null)
                    float(val),           # value_mw
                    SOURCE_NAME,          # source
                )
            )

    print(f"[INFO] Converted {len(records)} records for {bidding_zone}.")
    return records


def upsert_generation(records):
    if not records:
        print("[WARN] Nothing to upsert.")
        return

    print(f"[INFO] Connecting to TimescaleDB at {PG_HOST}:{PG_PORT}/{PG_DB}")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode=PG_SSLMODE,
    )

    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO generation_ts
                    (time_utc, bidding_zone, fuel_type, fuel_detail, value_mw, source)
                VALUES %s
                ON CONFLICT (time_utc, bidding_zone, fuel_type, fuel_detail, source)
                DO UPDATE SET
                    value_mw   = EXCLUDED.value_mw,
                    created_at = now();
            """
            execute_values(cur, sql, records, page_size=10_000)
        conn.commit()
        print(f"[INFO] Upserted {len(records)} rows.")
    finally:
        conn.close()


def main():
    print("=== ENTSO-E → Timescale import starting ===")

    now_utc = dt.datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=dt.timezone.utc
    )
    start_utc = now_utc - dt.timedelta(days=30)

    print(f"[INFO] Import window: {start_utc} → {now_utc}")

    for country_code, bidding_zone in ZONES:
        print(f"\n[ZONE] Processing {bidding_zone} ({country_code})")
        df = fetch_generation_df(country_code, bidding_zone, start_utc, now_utc)
        records = df_to_records(df, bidding_zone)
        upsert_generation(records)

    print("=== DONE ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[UNCAUGHT ERROR]")
        print(e)
        print("\n[TRACEBACK]")
        traceback.print_exc()

print(">>> BOTTOM OF SCRIPT REACHED")