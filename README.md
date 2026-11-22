# NOAA Teleconnections Climate Data Lakehouse
## Kiến trúc Medallion 3 Tầng: Bronze → Silver → Gold (2020 – 2022)

Kho dữ liệu chỉ số viễn liên khí hậu (NOAA/CPC Teleconnections: **AO, NAO, PNA, ENSO Niño 3.4**) được chuẩn hóa theo kiến trúc Medallion 3 tầng, đồng bộ 100% theo lịch thứ Ba hằng tuần của **U.S. Drought Monitor (USDM)** phục vụ phân tích, trực quan hóa và mô hình hóa dự báo hạn hán trên toàn lãnh thổ Hoa Kỳ (CONUS).

- **Người phụ trách**: Nguyễn Trung Kiên (NOAA Teleconnections)
- **Tham chiếu phương pháp**: Erhardt et al. (2024), *Scientific Data* 11, 375 — script gốc `TeleconnectionsForDryad.R` (`heplersa/USDMdata`)

---

## 1. Sơ đồ Tổng quan Hệ thống (System Architecture)

Toàn bộ dữ liệu và mã nguồn xử lý được tổ chức đồng bộ theo cấu trúc chuẩn của nhóm (`USDM_PQA_draftrepo`):

```text
NOAA_KienNT/
│
├── bronze/                         # TẦNG DỮ LIỆU THÔ (RAW INGESTION)
│   ├── data/                       # ingest_date=2026-10-17/ (3 CSV ngày + wksst9120.for + _manifest.json)
│   │                               # manifest_download.csv (nhật ký trạng thái tải về)
│   ├── scripts/                    # crawl_noaa_cpc.py, inspect_data.py
│   └── README.md                   # Hướng dẫn chi tiết tầng Bronze
│
├── silver/                         # TẦNG DỮ LIỆU CHUẨN HÓA (CONFORMED PARQUET)
│   ├── data/                       # climate_daily.parquet (1.461 ngày), enso_weekly.parquet (209 tuần)
│   │                               # quality_reports/ (quality_report.json, quality_report.csv)
│   │                               # manifests/ (manifest.json — Data Lineage SHA-256)
│   │                               # metadata/ (schema.json — đặc tả cột, đơn vị, mốc khí hậu)
│   ├── scripts/                    # noaa_bronze_to_silver.py, verify_silver_to_gold.py,
│   │                               # test_silver_pipeline.py, requirements.txt
│   └── README.md                   # Hướng dẫn chi tiết tầng Silver
│
├── gold/                           # TẦNG DỮ LIỆU TINH LUYỆN (ML-READY WEEKLY MART)
│   ├── data/                       # climate_weekly.parquet (130 tuần × 8 cột), climate_weekly.csv,
│   │                               # usdm_week_dates.csv, validation.txt,
│   │                               # comparison_reports/ (weekly_lineage.csv, verification_summary.json)
│   ├── scripts/                    # noaa_silver_to_gold.py, input_manifest.csv, environment_info.txt
│   └── README.md                   # Hướng dẫn chi tiết tầng Gold
│
├── references/                     # TÀI LIỆU & NGUỒN THAM CHIẾU NGHIÊN CỨU
│   └── README.md                   # Dryad dataset, GitHub heplersa/USDMdata, quy ước đối chiếu
│
├── requirements.txt                # Thư viện Python phụ thuộc (pandas, numpy, pyarrow, pytest)
├── .gitignore                      # Loại trừ file tạm Python/OS
└── README.md                       # TÀI LIỆU KIẾN TRÚC TỔNG THỂ
```

---

## 2. Bảng đối chiếu các tầng dữ liệu (Data Pipeline Matrix)

| Tiêu chí | Bronze Layer | Silver Layer | Gold Layer |
| :--- | :--- | :--- | :--- |
| **Thư mục tích hợp** | `bronze/` | `silver/` | `gold/` |
| **Dữ liệu (`data/`)** | 3 CSV ngày (`AO/NAO/PNA`) + 1 fixed-width `.for` (`wksst9120.for`) | Parquet (Snappy): `climate_daily.parquet` + `enso_weekly.parquet` | Parquet & CSV: `climate_weekly.parquet`, `climate_weekly.csv` |
| **Mã nguồn (`scripts/`)** | `crawl_noaa_cpc.py`, `inspect_data.py` | `noaa_bronze_to_silver.py`, `verify_silver_to_gold.py`, `test_silver_pipeline.py` | `noaa_silver_to_gold.py` |
| **Độ chi tiết (Granularity)** | Theo ngày (`AO/NAO/PNA`) & theo tuần thứ Tư (`ENSO`), vùng đệm 2019–2022 | 1.461 ngày liên tục (`climate_daily`) + 209 thứ Tư (`enso_weekly`) | 130 tuần thứ Ba (07/01/2020 – 28/06/2022), khớp 100% USDM |
| **Phạm vi không gian** | Toàn cầu / chỉ số khí hậu theo thời gian (không phụ thuộc ô lưới) | Theo thời gian (không lưu lặp theo ô lưới) | 1 dòng/tuần, ghép theo `week_date` sang lưới USDM khi mô hình hóa |
| **Kiểm tra chất lượng** | Xác thực SHA-256 gốc & lưu trữ, giữ nguyên định dạng `.csv`/`.for` | Chặn ngày khuyết, trùng lặp, NaN, giá trị sentinel NOAA (`\|x\| ≥ 99`), tách đúng âm dính nhau (`25.7-0.9`) | Cố định 130 dòng × 8 cột, 0 null, 0 trùng lặp, đối sánh Silver → Gold đạt 100% (`910/910` ô) |
| **Mục đích sử dụng** | Lưu trữ bất biến theo phân vùng `ingest_date=YYYY-MM-DD` | Single Source of Truth đã làm sạch, sẵn sàng tính rolling | Feature khí hậu cho mô hình dự báo hạn hán (GNN, LSTM, XGBoost...) |

---

## 3. Hướng dẫn vận hành nhanh (Quickstart)

Tất cả các script đều **tự động nhận diện thư mục gốc của repository** (`repo_root`), có thể chạy trực tiếp từ thư mục gốc `NOAA_KienNT`:

```bash
cd NOAA_KienNT

# Cài đặt thư viện (nếu chưa có)
python3 -m pip install -r requirements.txt

# --- 1. TẦNG BRONZE: Kiểm tra toàn vẹn SHA-256 & cấu trúc dữ liệu thô ---
python3 bronze/scripts/inspect_data.py
# (Tùy chọn) Thu thập snapshot mới theo ngày hiện tại:
# python3 bronze/scripts/crawl_noaa_cpc.py --start-date 2019-01-01 --end-date 2022-12-31

# --- 2. TẦNG SILVER: Chạy Unit Tests và pipeline Bronze -> Silver ---
python3 silver/scripts/test_silver_pipeline.py -v
python3 silver/scripts/noaa_bronze_to_silver.py --overwrite

# --- 3. TẦNG GOLD: Tổng hợp chuỗi tuần khớp lịch USDM ---
python3 gold/scripts/noaa_silver_to_gold.py --overwrite

# --- 4. KIỂM CHỨNG: Đối sánh 100% Silver -> Gold (và lịch tuần USDM) ---
python3 silver/scripts/verify_silver_to_gold.py --weeks 20200107 20210601 20220628
# Nếu có repo USDM cạnh bên, kiểm chứng chéo trực tiếp với usdmfinal.csv:
# python3 silver/scripts/verify_silver_to_gold.py --usdm-csv ../USDM_PQA_draftrepo/gold/data/usdmfinal.csv
```

---

## 4. Quy ước ghép nối với dữ liệu USDM Gold

Bảng `gold/data/climate_weekly.parquet` lưu **1 dòng cho mỗi tuần** (không lặp lại theo 3.259 ô lưới để tối ưu lưu trữ). Khi ghép với `usdmfinal.csv` (`time` dạng `YYYYMMDD`), chỉ cần chuyển `week_date` sang `YYYYMMDD` (hoặc parse `time` sang `datetime`) rồi `left join` theo khóa tuần:

```python
import pandas as pd

usdm = pd.read_csv("usdmfinal.csv", dtype={"time": str})
usdm["week_date"] = pd.to_datetime(usdm["time"], format="%Y%m%d")
noaa = pd.read_parquet("gold/data/climate_weekly.parquet")

merged = usdm.merge(noaa, on="week_date", how="left", validate="many_to_one")
```
