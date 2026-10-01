import os
import csv
import psycopg2
from psycopg2.extras import execute_values
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env")
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("OLTP_DB_NAME", "retail_oltp")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME,
    )


def load_stores(cursor):
    store_record = (
        "STR-001",
        "Grand Retail Supermarket",
        "Jl. Malioboro No. 12",
        "Yogyakarta",
    )
    sql = """
        INSERT INTO stores (store_id, store_name, address, city)
        VALUES %s
        ON CONFLICT (store_id) DO NOTHING;
    """
    execute_values(cursor, sql, [store_record])
    print("[OK] Stores loaded: 1 store")


def load_cashiers(cursor):
    cashiers_data = [
        ("CSH-001", "Budi Santoso", "Senior", "MORNING", 1, 0.015),
        ("CSH-002", "Siti Rahma", "Junior", "EVENING", 2, 0.040),
        ("CSH-003", "Andi Wijaya", "Senior", "MORNING", 3, 0.018),
        ("CSH-004", "Dewi Lestari", "Junior", "EVENING", 4, 0.045),
        ("CSH-005", "Rian Pratama", "Senior", "MORNING", 1, 0.015),
        ("CSH-006", "Maya Putri", "Junior", "EVENING", 2, 0.038),
        ("CSH-007", "Eko Prasetyo", "Senior", "MORNING", 3, 0.020),
        ("CSH-008", "Nanda Kartika", "Junior", "EVENING", 4, 0.042),
    ]
    sql = """
        INSERT INTO cashiers (cashier_id, name, experience_level, default_shift, assigned_counter, base_error_rate)
        VALUES %s
        ON CONFLICT (cashier_id) DO UPDATE SET
            experience_level = EXCLUDED.experience_level,
            default_shift = EXCLUDED.default_shift,
            assigned_counter = EXCLUDED.assigned_counter;
    """
    execute_values(cursor, sql, cashiers_data)
    print(f"[OK] Cashiers loaded: {len(cashiers_data)} cashiers")


def load_products(cursor):
    catalog_path = os.path.join(RAW_DATA_DIR, "inventory_catalog.csv")
    products = []
    with open(catalog_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(
                (
                    row["sku"],
                    row["barcode"],
                    row["name"],
                    row["category"],
                    float(row["cost_price"]),
                    float(row["sell_price"]),
                    int(row["stock"]),
                )
            )

    sql = """
        INSERT INTO products (sku, barcode, product_name, category, cost_price, sell_price, stock_on_hand)
        VALUES %s
        ON CONFLICT (sku) DO UPDATE SET
            cost_price = EXCLUDED.cost_price,
            sell_price = EXCLUDED.sell_price,
            stock_on_hand = EXCLUDED.stock_on_hand;
    """
    execute_values(cursor, sql, products)
    print(f"[OK] Products loaded: {len(products)} products from catalog")


def ensure_all_item_skus_registered(cursor):
    cursor.execute("SELECT sku FROM products")
    registered_skus = {row[0] for row in cursor.fetchall()}

    items_path = os.path.join(RAW_DATA_DIR, "pos_transaction_items.csv")
    missing_skus = {}
    with open(items_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = row["sku"].strip()
            if sku not in registered_skus and sku not in missing_skus:
                unit_price = float(row.get("unit_price", 0.0))
                prod_name = row.get("product_name", "UNKNOWN ITEM")
                missing_skus[sku] = (
                    sku,
                    f"UNKNOWN-{sku}",
                    prod_name,
                    "UNREGISTERED_SKU",
                    unit_price * 0.7,
                    unit_price,
                    0,
                )

    if missing_skus:
        sql = """
            INSERT INTO products (sku, barcode, product_name, category, cost_price, sell_price, stock_on_hand)
            VALUES %s
            ON CONFLICT (sku) DO NOTHING;
        """
        execute_values(cursor, sql, list(missing_skus.values()))
        print(f"[OK] Automatically registered {len(missing_skus)} missing/typo item SKUs to maintain 3NF referential integrity")


def load_cashier_shifts(cursor):
    shifts_path = os.path.join(RAW_DATA_DIR, "cashier_shifts.csv")
    shifts = []
    with open(shifts_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            shifts.append(
                (
                    row["shift_id"],
                    row["cashier_id"],
                    int(row["counter_id"]),
                    row["date"],
                    row["shift_type"],
                    int(row["transactions_processed"]),
                    int(row["errors_occurred"]),
                    float(row["peak_fatigue"]),
                )
            )
    sql = """
        INSERT INTO cashier_shifts (shift_id, cashier_id, counter_id, shift_date, shift_type, transactions_processed, errors_occurred, peak_fatigue)
        VALUES %s
        ON CONFLICT (shift_id) DO NOTHING;
    """
    execute_values(cursor, sql, shifts)
    print(f"[OK] Cashier shifts loaded: {len(shifts)} records")


def load_orders_and_customers(cursor):
    tx_path = os.path.join(RAW_DATA_DIR, "pos_transactions.csv")
    customers_dict = {}
    orders = []

    with open(tx_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cust_id = row["customer_id"].strip() if row.get("customer_id") else None
            if cust_id and cust_id not in customers_dict:
                is_guest = "GUEST" in cust_id or cust_id == "NON-MEMBER"
                has_loyalty = False if is_guest else ("LOYALTY" in cust_id)
                phone = None if is_guest else f"0812{abs(hash(cust_id)) % 100000000:08d}"
                persona = "Guest / Non-Member" if is_guest else "General Shopper"
                customers_dict[cust_id] = (
                    cust_id,
                    persona,
                    has_loyalty,
                    phone,
                )

            orders.append(
                (
                    row["transaction_id"],
                    row["store_id"],
                    row["cashier_id"],
                    cust_id if cust_id else None,
                    row["timestamp"],
                    row["payment_method"],
                    float(row["total_amount"]),
                    int(row["item_count"]),
                    row["has_error"].lower() == "true",
                )
            )

    cust_sql = """
        INSERT INTO customers (customer_id, persona, has_loyalty, phone_number)
        VALUES %s
        ON CONFLICT (customer_id) DO NOTHING;
    """
    execute_values(cursor, cust_sql, list(customers_dict.values()))
    print(f"[OK] Customers loaded: {len(customers_dict)} customers")

    order_sql = """
        INSERT INTO orders (transaction_id, store_id, cashier_id, customer_id, timestamp, payment_method, total_amount, item_count, has_error)
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING;
    """
    execute_values(cursor, order_sql, orders)
    print(f"[OK] Orders loaded: {len(orders)} transactions")


def load_order_items(cursor):
    items_path = os.path.join(RAW_DATA_DIR, "pos_transaction_items.csv")
    items = []
    with open(items_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            items.append(
                (
                    row["item_id"],
                    row["transaction_id"],
                    int(row["sequence"]),
                    row["sku"],
                    row["product_name"],
                    int(row["quantity"]),
                    float(row["unit_price"]),
                    float(row["subtotal"]),
                    float(row["discount_applied"]),
                    row["is_void"].lower() == "true",
                    row["error_type"],
                )
            )

    sql = """
        INSERT INTO order_items (item_id, transaction_id, sequence, sku, product_name, quantity, unit_price, subtotal, discount_applied, is_void, error_type)
        VALUES %s
        ON CONFLICT (item_id) DO NOTHING;
    """
    execute_values(cursor, sql, items, page_size=2000)
    print(f"[OK] Order items loaded: {len(items)} items")


def main():
    print("=" * 60)
    print("STARTING INGESTION TO POSTGRESQL OLTP (retail_oltp)")
    print(f"Target: {DB_HOST}:{DB_PORT}/{DB_NAME} (User: {DB_USER})")
    print("=" * 60)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            load_stores(cur)
            load_cashiers(cur)
            load_products(cur)
            ensure_all_item_skus_registered(cur)
            load_cashier_shifts(cur)
            load_orders_and_customers(cur)
            load_order_items(cur)
            conn.commit()
            print("=" * 60)
            print("SUCCESS: ALL RAW DATA SUCCESSFULLY LOADED INTO OLTP (3NF)!")
            print("=" * 60)
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Ingestion failed: {e}")
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    main()
