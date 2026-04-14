import os
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)
from cryptography.hazmat.backends import default_backend
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def load_private_key():
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "./keys/rsa_key.p8")
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

    with open(key_path, "rb") as f:
        private_key = load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

    return private_key.private_bytes(
        Encoding.DER,
        PrivateFormat.PKCS8,
        NoEncryption(),
    )


def test_connection():
    print("🔄 Loading private key...")
    private_key_bytes = load_private_key()
    print("✓ Private key loaded")

    print("🔄 Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key_bytes,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "DEMO_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SSE_DM_CSG_RPT_PRD"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RENEWALS"),
        role=os.getenv("SNOWFLAKE_ROLE") or None,
    )
    print("✓ Connected to Snowflake!")

    print("\n🔄 Running test queries...")
    cursor = conn.cursor()

    # Test 1: Basic connectivity
    cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    row = cursor.fetchone()
    print(f"✓ User: {row[0]} | Role: {row[1]} | Warehouse: {row[2]}")

    # Test 2: RENEWALS schema
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW
        LIMIT 1
    """
    )
    print(f"✓ RENEWALS view accessible: {cursor.fetchone()[0]} rows")

    # Test 3: CSS - Customer Health (skipped until approval)
    # cursor.execute(
    #     """
    #     SELECT MAX(SNAPSHOT_DT)
    #     FROM SSE_DM_CSG_RPT_PRD.CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW
    # """
    # )
    # print(f"✓ CSS health snapshot: {cursor.fetchone()[0]}")

    # Test 4: CSS - Attrition by Account (skipped until approval)
    # cursor.execute(
    #     """
    #     SELECT MAX(SNAPSHOT_DT)
    #     FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
    # """
    # )
    # print(f"✓ CSS attrition snapshot: {cursor.fetchone()[0]}")

    # Test 5: CSS - Attrition by Opportunity (skipped until approval)
    # cursor.execute(
    #     """
    #     SELECT MAX(SNAPSHOT_DT)
    #     FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY
    # """
    # )
    # print(f"✓ CSS attrition oppty snapshot: {cursor.fetchone()[0]}")

    # Test 4: CIDM - Usage (skip CSS until approval)
    cursor.execute(
        """
        SELECT MAX(SNAPSHOT_DT)
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    """
    )
    latest_cidm_snapshot = cursor.fetchone()[0]
    print(f"✓ CIDM usage snapshot: {latest_cidm_snapshot}")

    import time

    t = time.time()
    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE SNAPSHOT_DT = '{latest_cidm_snapshot}'
        AND PROVISIONED > 0
    """
    )
    print(f"✓ CIDM pinned query: {cursor.fetchone()[0]} rows — took {time.time()-t:.2f}s")

    # Test RENEWALS snap view size + timing
    t = time.time()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW
    """
    )
    print(f"✓ SNAP view rows: {cursor.fetchone()[0]} — took {time.time()-t:.2f}s")

    # Inspect RENEWALS snap view columns for territory/geo/ATR/swing/AOV mapping
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'WV_CI_RENEWAL_OPTY_SNAP_VW'
        AND TABLE_SCHEMA = 'RENEWALS'
        AND (
            COLUMN_NAME LIKE '%TERR%'
            OR COLUMN_NAME LIKE '%GEO%'
            OR COLUMN_NAME LIKE '%AREA%'
            OR COLUMN_NAME LIKE '%REGION%'
            OR COLUMN_NAME LIKE '%ATR%'
            OR COLUMN_NAME LIKE '%SWING%'
            OR COLUMN_NAME LIKE '%FCAST%'
            OR COLUMN_NAME LIKE '%AOV%'
        )
        ORDER BY COLUMN_NAME
    """
    )
    print("✓ Candidate columns from WV_CI_RENEWAL_OPTY_SNAP_VW:")
    for row in cursor.fetchall():
        print(row[0])

    cursor.close()
    conn.close()
    print("\n🎉 All tests passed! Service account is fully working.")


if __name__ == "__main__":
    test_connection()
