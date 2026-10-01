# Retail ABM Simulator & End-to-End Data Engineering Pipeline

Platform simulasi ritel berbasis **Agent-Based Modelling (ABM)** dengan arsitektur perangkat lunak modular yang menghasilkan data transaksi harian multi-tabel sarat *human error*, sebagai fondasi proyek pembelajaran end-to-end Data Engineering (PostgreSQL OLTP, PySpark Data Cleansing, OLAP Star Schema, dan Apache Airflow).

---

## Arsitektur Sistem

```text
┌─────────────────────────────────────────────────────────────┐
│                       PRESENTATION                          │
│   Web UI Dashboard (HTML5, Modular CSS/JS, Chart.js)        │
│   - Playable ABM Parameter Sliders & Seasonality Controls   │
│   - Real-time KPI Stats, Charts, & POS Transaction Inspector│
└──────────────────────────────┬──────────────────────────────┘
                               │ HTTP / REST API
┌──────────────────────────────▼──────────────────────────────┐
│                    APPLICATION BACKEND                      │
│   Flask App Factory & Modular Blueprints (backend/)         │
│   - simulation_routes.py | catalog_routes.py | data_routes  │
└──────────────────────────────┬──────────────────────────────┘
                               │ Internal Engine Call
┌──────────────────────────────▼──────────────────────────────┐
│                  ABM SIMULATION CORE                        │
│   Discrete-Event Simulation Engine (simulation/)            │
│   - 4 Customer Personas (Quick Grab, Family, Budget, Impulse│
│   - Market Basket Affinities & Dynamic Queue Abandonment    │
│   - Cashier Fatigue Curves & Injected Human Error Matrix    │
└──────────────────────────────┬──────────────────────────────┘
                               │ Multi-table CSV Export
┌──────────────────────────────▼──────────────────────────────┐
│                      RAW LANDING ZONE                       │
│   Multi-table Transaction Logs (data/raw/)                  │
│   - pos_transactions.csv | pos_transaction_items.csv        │
│   - cashier_shifts.csv   | inventory_catalog.csv            │
└──────────────────────────────┬──────────────────────────────┘
                               │
               (Fase Data Engineering Lanjutan)
                               ▼
┌──────────────────────────────┬──────────────────────────────┐
│     POSTGRESQL (OLTP)        │     STAGING & CLEANSE        │
│   Normal Form 3NF Relational │   Deduplikasi, Karantina Typo│
│   (oltp_schema.sql)          │   & Filter Void via PySpark  │
└──────────────────────────────┴──────────────┬───────────────┘
                                              ▼
                               ┌──────────────────────────────┐
                               │      POSTGRESQL (OLAP)       │
                               │   Dimensional Star Schema    │
                               │   (olap_star_schema.sql)     │
                               └──────────────┬───────────────┘
                                              ▼
                               ┌──────────────────────────────┐
                               │   TABLEAU & TIME-SERIES      │
                               │   Business BI & Forecasting  │
                               └──────────────────────────────┘
```

---

## Status Proyek & Roadmap

### Yang Sudah Selesai (Completed)

- [x] **Agent-Based Modelling Engine (ABM):**
  - Pemodelan diskrit harian dari jam 08:00 sampai 22:00 dengan pola musiman (*Lunch Rush*, *Evening Peak*, *Weekend Boost*, dan *Payday Effect*).
  - 4 Persona Pembeli dengan perilaku belanja spesifik dan batasan kesabaran antrean.
  - Katalog 42 produk ritel dengan aturan afinitas keranjang (*Market Basket Affinity*).
  - Dinamika kasir (shift pagi/sore, kasir junior vs senior, kurva kelelahan berbasis jam kerja dan panjangnya antrean).
  - Injeksi 5 jenis *human error*: *Double Scan*, *Manual SKU Typo (Fat Finger)*, *Void Item*, *Missing Customer Member ID*, dan *Wrong Payment Method*.

- [x] **Kaidah Arsitektur Perangkat Lunak & Design Patterns:**
  - Penerapan **Strategy Pattern** untuk memisahkan strategi belanja pelanggan dan injeksi error kasir.
  - Penerapan **Factory Pattern** untuk instansiasi agen pembeli dan kasir.
  - Penerapan **Single Responsibility Principle (SRP)**: Seluruh file dipecah modular ke ukuran ramping (15–70 baris per file).
  - **Zero Comments Rule**: 100% kode bersih tanpa baris komentar di seluruh backend, frontend, dan database.
  - Pemisahan tegas struktur direktori tingkat atas (`backend/`, `frontend/`, `simulation/`, `data/`, `data_engineering/`).

- [x] **Interactive Web Dashboard & Discrete-Day Playback:**
  - Web UI monokromatik gelap, minimalis, dan elegan tanpa border berlebihan dan tanpa badge status kecil pada card.
  - **Discrete-Day Process Animation**: Simulasi berjalan langkah demi langkah per hari diskrit dengan timeline dan progress bar.
  - **Playback Controller (State Pattern)**: Tombol `Start/Play`, `Pause`, `Next Day (Step)`, dan `Reset`.
  - **Speed Control**: Pilihan kecepatan `1x` (observasi), `2x`, `5x`, dan `Instant` (selesai seketika).
  - **Store Floor & Cashier Counters**: Visualisasi 4 till counter dengan meteran kelelahan (*fatigue level bar*) dinamis dan counter transaksi kasir.
  - Visualisasi grafik *Chart.js* yang memanjang titik demi titik secara hidup seiring pergantian hari diskrit.
  - Panel kontrol variabel ABM interaktif (slider durasi, jumlah pembeli, distribusi persona, kelelahan, dan rasio kasir).
  - Tabel inspeksi transaksi POS dengan filter anomali dan dialog modal detail struk.
  - Download Hub untuk mengekspor data transaksi langsung dari browser.

- [x] **Desain Skema Database Data Engineering:**
  - Skema **PostgreSQL OLTP (3NF)**: DDL untuk tabel `stores`, `cashiers`, `customers`, `products`, `orders`, `order_items`, dan `cashier_shifts`.
  - Skema **PostgreSQL OLAP (Star Schema)**: DDL untuk tabel dimensi (`dim_date`, `dim_product`, `dim_cashier`, `dim_customer`) dan tabel fakta (`fact_sales`, `fact_cashier_daily_performance`).

---

### Yang Belum Dikerjakan (Roadmap / To-Do)

Bagian-bagian berikut adalah tahapan lanjutan untuk melengkapi pipeline Data Engineering:

- [x] **1. Script Ingestion ke PostgreSQL OLTP (`data_engineering/scripts/load_oltp.py`):**
  - Membaca file CSV dari `data/raw/` dan memasukkannya ke database PostgreSQL relasional (3NF) sesuai urutan *Foreign Key*.
- [x] **2. Script Pembersihan Data / Staging Cleansing (`data_engineering/scripts/cleanse_silver.py`):**
  - Mengambil data dari OLTP untuk dicuci menggunakan Python:
    - Mendeteksi dan menghapus duplikasi *double scan* kasir.
    - Mengkarantina transaksi dengan SKU salah ketik (*invalid SKU typo*).
    - Menghitung ulang omzet bersih dengan mengecualikan item yang dibatalkan (*void*).
    - Standarisasi format tanggal dan imputasi nilai kosong.
- [x] **3. Script Pemodelan Dimensi OLAP (`data_engineering/scripts/load_olap.py`):**
  - Mengisi tabel dimensi dan tabel fakta di database analitik (*Star Schema*).
- [x] **4. Otomatisasi dengan Apache Airflow DAG (`data_engineering/airflow/dags/retail_daily_etl_dag.py`):**
  - Menjadwalkan alur Ingest $\rightarrow$ Cleanse $\rightarrow$ Load $\rightarrow$ Data Quality Audit otomatis setiap jam 01:00 pagi.
- [ ] **5. Visualisasi Tableau & Model Prediksi Time-Series:**
  - Menyambungkan database OLAP ke Tableau untuk analisis bisnis (korelasi kelelahan kasir vs tingkat kesalahan).
  - Pembuatan model time-series (Prophet / LightGBM) untuk memprediksi omzet 30 hari ke depan.

---

## Struktur Direktori

```text
Porto_Data/
├── backend/
│   ├── routes/
│   │   ├── simulation_routes.py
│   │   ├── catalog_routes.py
│   │   └── data_routes.py
│   └── app_factory.py
├── frontend/
│   ├── templates/
│   │   └── index.html
│   └── static/
│       ├── css/
│       │   ├── variables.css
│       │   ├── base.css
│       │   ├── layout.css
│       │   ├── controls.css
│       │   ├── stats.css
│       │   ├── charts.css
│       │   ├── table.css
│       │   ├── export.css
│       │   ├── modal.css
│       │   └── style.css
│       └── js/
│           ├── api.js
│           ├── charts.js
│           ├── controls.js
│           ├── table.js
│           ├── modal.js
│           └── app.js
├── simulation/
│   ├── domain/
│   │   ├── enums.py
│   │   ├── product.py
│   │   ├── customer.py
│   │   ├── cashier.py
│   │   ├── transaction.py
│   │   ├── shift.py
│   │   └── config.py
│   ├── strategies/
│   │   ├── customer_base.py
│   │   ├── customer_quick_grab.py
│   │   ├── customer_family.py
│   │   ├── customer_budget.py
│   │   ├── customer_impulse.py
│   │   ├── customer_strategy_factory.py
│   │   ├── error_base.py
│   │   ├── error_double_scan.py
│   │   ├── error_typo.py
│   │   ├── error_void.py
│   │   └── error_engine.py
│   ├── factories/
│   │   ├── customer_factory.py
│   │   └── cashier_factory.py
│   ├── catalog.py
│   ├── exporter.py
│   └── engine.py
├── data/
│   └── raw/
│       ├── pos_transactions.csv
│       ├── pos_transaction_items.csv
│       ├── cashier_shifts.csv
│       ├── inventory_catalog.csv
│       └── simulation_metrics.json
├── data_engineering/
│   ├── airflow/
│   │   └── dags/
│   │       └── retail_daily_etl_dag.py
│   ├── schemas/
│   │   ├── oltp_schema.sql
│   │   └── olap_star_schema.sql
│   ├── scripts/
│   │   ├── load_oltp.py
│   │   ├── cleanse_silver.py
│   │   ├── load_olap.py
│   │   └── run_pipeline.py
│   └── README.md
├── docker/
│   └── init-db.sql
├── docker-compose.yml
├── .env.example
├── main.py
└── README.md
```

---

## Cara Menjalankan Aplikasi & Data Pipeline

### 1. ABM Simulation Web Dashboard (Local Flask)
```powershell
python main.py
```
- Akses UI di: `http://127.0.0.1:5000`
- Mainkan slider simulasi, atur pola hari diskrit, dan unduh data transaksi CSV.

### 2. Containerized Data Engineering Stack (Docker)
Salin `.env.example` ke `.env` jika belum ada:
```powershell
cp .env.example .env
docker compose up -d
```
Layanan aktif:
- **PostgreSQL 16 Engine**: `localhost:5433` (Databases: `retail_oltp`, `retail_olap`, `retail_airflow`)
- **pgAdmin 4 Web GUI**: [http://localhost:5050](http://localhost:5050) (User: `admin@retail.com`, Pass: `admin123`)
- **Apache Airflow 2.9.3 Web UI**: [http://localhost:8080](http://localhost:8080) (User: `admin`, Pass: `admin`)

### 3. Eksekusi End-to-End ETL Pipeline
Kamu dapat menjalankan pipeline lewat CLI mandiri atau lewat Airflow:

**Opsi A — CLI Runner Langsung:**
```powershell
python data_engineering/scripts/run_pipeline.py
```

**Opsi B — Apache Airflow Web UI:**
Buka [http://localhost:8080](http://localhost:8080), aktifkan DAG `retail_daily_etl_pipeline`, dan klik **Trigger DAG**. Seluruh tahapan (`ingest_raw_to_oltp` $\rightarrow$ `cleanse_and_stage_silver` $\rightarrow$ `load_dimensional_olap` $\rightarrow$ `data_quality_audit`) akan berjalan terjadwal secara otomatis.
