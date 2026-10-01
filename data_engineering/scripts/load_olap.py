import os
import csv
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
OLAP_DB_NAME = os.getenv("OLAP_DB_NAME", "retail_olap")
OLTP_DB_NAME = os.getenv("OLTP_DB_NAME", "retail_oltp")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")


def get_connection(dbname):
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=dbname,
    )


def populate_dim_date(cur_olap, unique_dates):
    date_records = []
    for dt_str in sorted(unique_dates):
        dt = datetime.strptime(dt_str, "%Y-%m-%d")
        date_key = int(dt.strftime("%Y%m%d"))
        day_of_week = dt.isoweekday()
        day_name = dt.strftime("%A")
        day_of_month = dt.day
        month = dt.month
        month_name = dt.strftime("%B")
        quarter = (month - 1) // 3 + 1
        year = dt.year
        is_weekend = day_of_week in (6, 7)
        is_payday = day_of_month in (1, 2, 25, 26, 27, 28, 29, 30, 31)

        date_records.append(
            (
                date_key,
                dt.date(),
                day_of_week,
                day_name,
                day_of_month,
                month,
                month_name,
                quarter,
                year,
                is_weekend,
                is_payday,
            )
        )

    sql = """
        INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, month, month_name, quarter, year, is_weekend, is_payday)
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING;
    """
    execute_values(cur_olap, sql, date_records)
    print(f"[OK] dim_date populated: {len(date_records)} calendar dates")


def populate_dim_product(cur_olap, cur_oltp):
    cur_oltp.execute("SELECT sku, barcode, product_name, category, cost_price, sell_price FROM products WHERE category != 'UNREGISTERED_SKU';")
    products = cur_oltp.fetchall()

    product_records = []
    for p in products:
        cost = float(p["cost_price"])
        sell = float(p["sell_price"])
        margin_amount = round(sell - cost, 2)
        margin_pct = round((margin_amount / sell) * 100, 2) if sell > 0 else 0.0
        product_records.append(
            (
                p["sku"],
                p["barcode"],
                p["product_name"],
                p["category"],
                cost,
                sell,
                margin_amount,
                margin_pct,
            )
        )

    sql = """
        INSERT INTO dim_product (sku, barcode, product_name, category, cost_price, sell_price, margin_amount, margin_percentage)
        VALUES %s
        RETURNING product_key, sku;
    """
    cur_olap.execute("TRUNCATE dim_product RESTART IDENTITY CASCADE;")
    execute_values(cur_olap, sql, product_records)
    print(f"[OK] dim_product populated: {len(product_records)} active retail products")


def populate_dim_cashier(cur_olap, cur_oltp):
    cur_oltp.execute("SELECT cashier_id, name, experience_level, default_shift FROM cashiers ORDER BY cashier_id;")
    cashiers = cur_oltp.fetchall()

    cashier_records = [
        (c["cashier_id"], c["name"], c["experience_level"], c["default_shift"])
        for c in cashiers
    ]

    cur_olap.execute("TRUNCATE dim_cashier RESTART IDENTITY CASCADE;")
    sql = """
        INSERT INTO dim_cashier (cashier_id, name, experience_level, default_shift)
        VALUES %s;
    """
    execute_values(cur_olap, sql, cashier_records)
    print(f"[OK] dim_cashier populated: {len(cashier_records)} cashiers")


def populate_dim_customer(cur_olap, cur_oltp):
    cur_oltp.execute("SELECT customer_id, persona, has_loyalty FROM customers ORDER BY customer_id;")
    customers = cur_oltp.fetchall()

    customer_records = [("CUST-GUEST", "Guest / Non-Member", False)]
    for c in customers:
        if c["customer_id"] != "CUST-GUEST":
            customer_records.append((c["customer_id"], c["persona"], c["has_loyalty"]))

    cur_olap.execute("TRUNCATE dim_customer RESTART IDENTITY CASCADE;")
    sql = """
        INSERT INTO dim_customer (customer_id, persona, has_loyalty)
        VALUES %s;
    """
    execute_values(cur_olap, sql, customer_records, page_size=2000)
    print(f"[OK] dim_customer populated: {len(customer_records)} customer personas")


def get_dimension_lookup_maps(cur_olap):
    cur_olap.execute("SELECT sku, product_key FROM dim_product;")
    product_map = {row["sku"]: row["product_key"] for row in cur_olap.fetchall()}

    cur_olap.execute("SELECT cashier_id, cashier_key FROM dim_cashier;")
    cashier_map = {row["cashier_id"]: row["cashier_key"] for row in cur_olap.fetchall()}

    cur_olap.execute("SELECT customer_id, customer_key FROM dim_customer;")
    customer_map = {row["customer_id"]: row["customer_key"] for row in cur_olap.fetchall()}

    guest_key = customer_map.get("CUST-GUEST", 1)

    return product_map, cashier_map, customer_map, guest_key


def load_fact_sales(cur_olap, product_map, cashier_map, customer_map, guest_key):
    clean_csv_path = os.path.join(SILVER_DIR, "clean_sales.csv")
    if not os.path.exists(clean_csv_path):
        raise FileNotFoundError(f"Clean sales CSV not found at {clean_csv_path}. Run cleanse_silver.py first.")

    fact_records = []
    with open(clean_csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_str = row["timestamp"]
            date_key = int(ts_str[:10].replace("-", ""))

            p_key = product_map.get(row["sku"])
            c_key = cashier_map.get(row["cashier_id"])
            cust_key = customer_map.get(row["customer_id"], guest_key)

            if p_key is None or c_key is None:
                continue

            fact_records.append(
                (
                    date_key,
                    p_key,
                    c_key,
                    cust_key,
                    row["transaction_id"],
                    row["item_id"],
                    row["payment_method"],
                    int(row["quantity"]),
                    float(row["unit_price"]),
                    float(row["cost_price"]),
                    float(row["gross_revenue"]),
                    float(row["discount_amount"]),
                    float(row["net_revenue"]),
                    float(row["gross_profit"]),
                    False,
                    "NONE",
                )
            )

    cur_olap.execute("TRUNCATE fact_sales RESTART IDENTITY;")
    sql = """
        INSERT INTO fact_sales (
            date_key, product_key, cashier_key, customer_key,
            transaction_id, item_id, payment_method, quantity, unit_price, cost_price,
            gross_revenue, discount_amount, net_revenue, gross_profit, is_void, error_flag
        ) VALUES %s;
    """
    execute_values(cur_olap, sql, fact_records, page_size=2000)
    print(f"[OK] fact_sales populated: {len(fact_records)} clean sales facts inserted")


def load_fact_cashier_performance(cur_olap, cur_oltp, cashier_map):
    cur_oltp.execute("""
        SELECT 
            shift_id, cashier_id, counter_id, shift_date, shift_type, 
            transactions_processed, errors_occurred, peak_fatigue 
        FROM cashier_shifts
        ORDER BY shift_date, cashier_id;
    """)
    shifts = cur_oltp.fetchall()

    perf_records = []
    for s in shifts:
        date_key = int(str(s["shift_date"]).replace("-", ""))
        c_key = cashier_map.get(s["cashier_id"])
        if c_key is None:
            continue

        tx_count = s["transactions_processed"]
        err_count = s["errors_occurred"]
        err_rate = round((err_count / tx_count) * 100, 2) if tx_count > 0 else 0.0

        perf_records.append(
            (
                date_key,
                c_key,
                s["counter_id"],
                s["shift_type"],
                tx_count,
                err_count,
                err_rate,
                float(s["peak_fatigue"]),
            )
        )

    cur_olap.execute("TRUNCATE fact_cashier_daily_performance RESTART IDENTITY;")
    sql = """
        INSERT INTO fact_cashier_daily_performance (
            date_key, cashier_key, counter_id, shift_type,
            transactions_processed, errors_occurred, error_rate_pct, peak_fatigue
        ) VALUES %s;
    """
    execute_values(cur_olap, sql, perf_records)
    print(f"[OK] fact_cashier_daily_performance populated: {len(perf_records)} cashier daily performance records")


def run_olap_loader():
    print("=" * 65)
    print("STEP 3: LOADING INTO OLAP DATA WAREHOUSE (STAR SCHEMA)")
    print(f"Target Database: {OLAP_DB_NAME} at {DB_HOST}:{DB_PORT}")
    print("=" * 65)

    conn_oltp = get_connection(OLTP_DB_NAME)
    conn_olap = get_connection(OLAP_DB_NAME)

    try:
        cur_oltp = conn_oltp.cursor(cursor_factory=RealDictCursor)
        cur_olap = conn_olap.cursor(cursor_factory=RealDictCursor)

        clean_csv_path = os.path.join(SILVER_DIR, "clean_sales.csv")
        unique_dates = set()
        with open(clean_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                unique_dates.add(row["timestamp"][:10])

        print("\n--- POPULATING DIMENSION TABLES ---")
        populate_dim_date(cur_olap, unique_dates)
        populate_dim_product(cur_olap, cur_oltp)
        populate_dim_cashier(cur_olap, cur_oltp)
        populate_dim_customer(cur_olap, cur_oltp)

        print("\n--- BUILDING SURROGATE KEY MAPPINGS ---")
        product_map, cashier_map, customer_map, guest_key = get_dimension_lookup_maps(cur_olap)

        print("\n--- POPULATING FACT TABLES (STAR SCHEMA CENTERS) ---")
        load_fact_sales(cur_olap, product_map, cashier_map, customer_map, guest_key)
        load_fact_cashier_performance(cur_olap, cur_oltp, cashier_map)

        conn_olap.commit()
        print("\n" + "=" * 65)
        print("SUCCESS: OLAP STAR SCHEMA DATA WAREHOUSE LOADED 100%!")
        print("=" * 65)

    except Exception as e:
        conn_olap.rollback()
        print(f"[ERROR] OLAP loading failed: {e}")
        raise e
    finally:
        conn_oltp.close()
        conn_olap.close()


if __name__ == "__main__":
    run_olap_loader()
