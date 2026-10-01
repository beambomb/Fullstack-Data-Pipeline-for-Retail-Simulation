import os
import csv
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("OLTP_DB_NAME", "retail_oltp")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
os.makedirs(SILVER_DIR, exist_ok=True)


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
    )


def extract_raw_orders_and_items(conn):
    query = """
        SELECT 
            oi.item_id,
            oi.transaction_id,
            oi.sequence,
            oi.sku,
            oi.product_name,
            oi.quantity,
            oi.unit_price,
            oi.subtotal,
            oi.discount_applied,
            oi.is_void,
            oi.error_type,
            o.timestamp,
            o.store_id,
            o.cashier_id,
            o.customer_id,
            o.payment_method,
            p.category,
            p.cost_price,
            p.sell_price
        FROM order_items oi
        JOIN orders o ON oi.transaction_id = o.transaction_id
        LEFT JOIN products p ON oi.sku = p.sku
        ORDER BY o.timestamp, oi.transaction_id, oi.sequence;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return rows


def run_cleansing():
    print("=" * 65)
    print("STEP 2: RUNNING DATA CLEANSING & PREPROCESSING (SILVER LAYER)")
    print(f"Source: PostgreSQL OLTP ({DB_NAME})")
    print(f"Target Output: {SILVER_DIR}")
    print("=" * 65)

    conn = get_connection()
    try:
        raw_items = extract_raw_orders_and_items(conn)
        print(f"[EXTRACT] Successfully fetched {len(raw_items)} raw order items from OLTP.")

        clean_items = []
        quarantine_records = []
        stats = {
            "total_raw": len(raw_items),
            "void_filtered": 0,
            "double_scan_deduped": 0,
            "typo_quarantined": 0,
            "clean_promoted": 0,
            "raw_revenue": 0.0,
            "clean_revenue": 0.0,
            "clean_profit": 0.0,
        }

        seen_scans = set()

        for row in raw_items:
            qty = row["quantity"]
            unit_price = float(row["unit_price"])
            subtotal = float(row["subtotal"])
            cost_price = float(row["cost_price"]) if row["cost_price"] is not None else 0.0
            stats["raw_revenue"] += subtotal

            if row["is_void"] or row["error_type"] == "VOID_ITEM":
                stats["void_filtered"] += 1
                quarantine_records.append({
                    "item_id": row["item_id"],
                    "transaction_id": row["transaction_id"],
                    "sku": row["sku"],
                    "product_name": row["product_name"],
                    "error_reason": "VOID_ITEM_CANCELLED_BY_CUSTOMER",
                    "timestamp": row["timestamp"],
                    "cashier_id": row["cashier_id"],
                    "amount_lost": subtotal,
                })
                continue

            scan_fingerprint = (row["transaction_id"], row["sku"])
            if row["error_type"] == "DOUBLE_SCAN" and scan_fingerprint in seen_scans:
                stats["double_scan_deduped"] += 1
                quarantine_records.append({
                    "item_id": row["item_id"],
                    "transaction_id": row["transaction_id"],
                    "sku": row["sku"],
                    "product_name": row["product_name"],
                    "error_reason": "DOUBLE_SCAN_DUPLICATE_REMOVED",
                    "timestamp": row["timestamp"],
                    "cashier_id": row["cashier_id"],
                    "amount_lost": subtotal,
                })
                continue
            seen_scans.add(scan_fingerprint)

            if row["error_type"] == "TYPO_SKU" or row["category"] == "UNREGISTERED_SKU" or not row["sku"].startswith("SKU-"):
                stats["typo_quarantined"] += 1
                quarantine_records.append({
                    "item_id": row["item_id"],
                    "transaction_id": row["transaction_id"],
                    "sku": row["sku"],
                    "product_name": row["product_name"],
                    "error_reason": "TYPO_SKU_QUARANTINED_FOR_AUDIT",
                    "timestamp": row["timestamp"],
                    "cashier_id": row["cashier_id"],
                    "amount_lost": subtotal,
                })
                continue

            discount = float(row["discount_applied"])
            gross_revenue = round(qty * unit_price, 2)
            net_revenue = round(gross_revenue - discount, 2)
            gross_profit = round(net_revenue - (qty * cost_price), 2)

            stats["clean_promoted"] += 1
            stats["clean_revenue"] += net_revenue
            stats["clean_profit"] += gross_profit

            clean_items.append({
                "item_id": row["item_id"],
                "transaction_id": row["transaction_id"],
                "timestamp": str(row["timestamp"]),
                "store_id": row["store_id"],
                "cashier_id": row["cashier_id"],
                "customer_id": row["customer_id"] if row["customer_id"] else "CUST-GUEST",
                "payment_method": row["payment_method"],
                "sku": row["sku"],
                "product_name": row["product_name"],
                "category": row["category"],
                "quantity": qty,
                "unit_price": unit_price,
                "cost_price": cost_price,
                "gross_revenue": gross_revenue,
                "discount_amount": discount,
                "net_revenue": net_revenue,
                "gross_profit": gross_profit,
                "margin_pct": round((gross_profit / net_revenue) * 100, 2) if net_revenue > 0 else 0.0,
            })

        clean_csv_path = os.path.join(SILVER_DIR, "clean_sales.csv")
        clean_fieldnames = list(clean_items[0].keys())
        with open(clean_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=clean_fieldnames)
            writer.writeheader()
            writer.writerows(clean_items)

        quarantine_csv_path = os.path.join(SILVER_DIR, "quarantine_audit_log.csv")
        if quarantine_records:
            quarantine_fieldnames = list(quarantine_records[0].keys())
            with open(quarantine_csv_path, mode="w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=quarantine_fieldnames)
                writer.writeheader()
                writer.writerows(quarantine_records)

        print("\n" + "=" * 65)
        print("EXECUTIVE DATA QUALITY & CLEANSING REPORT")
        print("=" * 65)
        print(f"Total Raw Items Ingested     : {stats['total_raw']:,} rows")
        print(f"[-] Void Items Filtered       : {stats['void_filtered']:,} rows (Pembeli membatalkan barang)")
        print(f"[-] Double Scans Deduplicated : {stats['double_scan_deduped']:,} rows (Kasir lelah scan ganda)")
        print(f"[-] Typo SKUs Quarantined     : {stats['typo_quarantined']:,} rows (Salah ketik manual barcode)")
        print(f"[=] Clean Items (Silver Layer): {stats['clean_promoted']:,} rows ({stats['clean_promoted']/stats['total_raw']*100:.1f}%)")
        print("-" * 65)
        print(f"Raw Revenue (dengan error)   : Rp {stats['raw_revenue']:,.2f}")
        print(f"Clean Net Revenue (Nyata)    : Rp {stats['clean_revenue']:,.2f}")
        print(f"Clean Gross Profit (Untung)  : Rp {stats['clean_profit']:,.2f}")
        print(f"Clean Overall Profit Margin  : {(stats['clean_profit']/stats['clean_revenue']*100 if stats['clean_revenue'] > 0 else 0):.2f}%")
        print("=" * 65)
        print(f"[SUCCESS] Clean dataset saved to      : {clean_csv_path}")
        print(f"[SUCCESS] Quarantine log saved to     : {quarantine_csv_path}")
        print("=" * 65)

    finally:
        conn.close()


if __name__ == "__main__":
    run_cleansing()
