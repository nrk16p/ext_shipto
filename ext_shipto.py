import os
import requests
import io
import pandas as pd
import warnings
import urllib3
from typing import List
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# CONFIG (FROM ENV)
# ============================================================

PHPSESSID = os.getenv("ATMS_PHPSESSID")
if not PHPSESSID:
    raise RuntimeError("❌ ATMS_PHPSESSID is required")

ATMS_URL = "https://www.mena-atms.com/report/excel/index.excel/type/ship.to"

ATMS_HEADERS = {
    "Referer": ATMS_URL,
    "Content-Type": "application/x-www-form-urlencoded",
    "Cookie": f"PHPSESSID={PHPSESSID}",
}

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("❌ MONGO_URI is required")

MONGO_DB = "atms"
MONGO_COL = "shipto"

CUSTOMER_IDS = os.getenv("CUSTOMER_IDS", "19").split(",")

warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# ATMS FETCH
# ============================================================

def fetch_ship_to(customer_id: str) -> pd.DataFrame:
    payload = {
        "code": "",
        "sub_code": "",
        "name": "",
        "amphur": "",
        "province": "",
        "country_id": "",
        "customer_id": customer_id,
        "zone_id": "",
        "traffic_id": "",
        "status": "A",
        "from_updated_date": "",
        "to_updated_date": "",
        "from_valid_date": "",
        "to_valid_date": "",
        "submit": "พิมพ์",
        "report_type": "ship.to"
    }

    session = requests.Session()

    retries = Retry(
        total=3,
        backoff_factor=10,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    resp = session.post(
        ATMS_URL,
        data=payload,
        headers=ATMS_HEADERS,
        verify=False,
        timeout=(10, 300000)   # connect / read
    )

    resp.raise_for_status()

    with io.BytesIO(resp.content) as f:
        df = pd.read_excel(f, sheet_name=0, dtype=str, skiprows=1)

    df.columns = df.columns.str.strip()
    df = df.dropna(how="all")
    df = df[df.iloc[:, 0] != df.columns[0]].reset_index(drop=True)

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    df["customer_id"] = customer_id
    return df

# ============================================================
# MONGO UPSERT
# ============================================================

def upsert_shipto(df: pd.DataFrame):
    if df.empty:
        print("⚠️ Empty DataFrame — skip")
        return

    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COL]

    now = datetime.utcnow()
    ops = []

    for _, row in df.iterrows():
        doc = row.to_dict()
        ship_to_code = doc.get("รหัส") or doc.get("ShipToCode")
        customer_id = doc.get("customer_id")

        if not ship_to_code:
            continue

        ops.append(
            UpdateOne(
                {"customer_id": customer_id, "ship_to_code": ship_to_code},
                {
                    "$set": {
                        **doc,
                        "ship_to_code": ship_to_code,
                        "updated_at": now,
                        "source": "mena-atms",
                        "is_active": True
                    },
                    "$setOnInsert": {"created_at": now}
                },
                upsert=True
            )
        )

    if ops:
        result = col.bulk_write(ops, ordered=False)
        print(f"✅ UPSERT done | inserted={result.upserted_count}, modified={result.modified_count}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("🚀 ATMS ShipTo Sync Started")
    print("Customers:", CUSTOMER_IDS)

    for cid in CUSTOMER_IDS:
        print(f"\n📥 Fetch customer {cid}")
        df = fetch_ship_to(cid)
        print("Rows:", len(df))
        upsert_shipto(df)

    print("\n🏁 JOB COMPLETED")

if __name__ == "__main__":
    main()
