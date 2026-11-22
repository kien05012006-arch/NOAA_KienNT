# Silver Layer (Cleaned & Conformed Parquet)
## NOAA Teleconnections (Buffered 2019 – 2022)

Tầng dữ liệu chuẩn hóa (**Single Source of Truth**) lưu trữ các bảng chuỗi thời gian khí hậu đã làm sạch, chuẩn hóa kiểu ngày và kiểm định chặt chẽ tính liên tục — **chưa áp dụng phép tổng hợp nghiệp vụ** (trung bình trượt 7 ngày, dịch thứ Tư về thứ Ba thuộc trách nhiệm tầng Gold).

---

## 1. Cấu trúc thư mục

```text
silver/
├── data/
│   ├── climate_daily.parquet          # Bảng ngày hợp nhất AO, NAO, PNA (1.461 dòng, Snappy)
│   ├── enso_weekly.parquet            # Bảng tuần ENSO Niño 3.4 SST & SSTA (209 dòng, Snappy)
│   ├── quality_reports/
│   │   ├── quality_report.json        # Báo cáo kiểm định chất lượng chi tiết
│   │   └── quality_report.csv         # Tóm tắt kiểm định dạng bảng
│   ├── manifests/
│   │   └── manifest.json              # Nhật ký thực thi & Data Lineage (SHA-256 Bronze -> Silver)
│   └── metadata/
│       └── schema.json                # Đặc tả schema, đơn vị và mốc khí hậu
├── scripts/
│   ├── noaa_bronze_to_silver.py       # Pipeline xử lý Bronze -> Silver Parquet
│   ├── verify_silver_to_gold.py       # Kiểm chứng tái tạo 100% Gold từ Silver
│   ├── test_silver_pipeline.py        # Unit test suite (parser, validation, rolling)
│   └── requirements.txt               # Thư viện Python phụ thuộc
└── README.md
```

---

## 2. Đặc tả dữ liệu

### 2.1. Bảng `climate_daily.parquet`
- **Định dạng**: Apache Parquet (nén Snappy).
- **Số dòng**: **1.461 dòng** liên tục theo ngày (`2019-01-01` → `2022-12-31`), 0 trùng lặp, 0 null.
- **Schema**:
  - `date` (`datetime64[ns]`): Khóa chính, ngày quan trắc.
  - `ao` (`float64`): Chỉ số Arctic Oscillation (CDAS z1000).
  - `nao` (`float64`): Chỉ số North Atlantic Oscillation (CDAS z500).
  - `pna` (`float64`): Chỉ số Pacific/North American (CDAS z500).

### 2.2. Bảng `enso_weekly.parquet`
- **Định dạng**: Apache Parquet (nén Snappy).
- **Số dòng**: **209 dòng** liên tục cách đều 7 ngày (`2019-01-02` → `2022-12-28`), 100% là thứ Tư (`dayofweek == 2`), 0 trùng lặp, 0 null.
- **Schema**:
  - `source_week_date` (`datetime64[ns]`): Khóa chính, ngày thứ Tư gốc của NOAA CPC.
  - `nino34_sst` (`float64`): Nhiệt độ mặt biển tuyệt đối vùng Niño 3.4 (°C).
  - `nino34_ssta` (`float64`): Dị thường nhiệt độ mặt biển vùng Niño 3.4 (°C, mốc 1991–2020).

### 2.3. Quy tắc kiểm định chất lượng (Quality Gates)
- Tự động tách chính xác các giá trị âm dính liền trong `wksst9120.for` (ví dụ `25.7-0.9` → `25.7` và `-0.9`); hỗ trợ cả định dạng 3 cột đã cắt và định dạng 9 cột nguyên bản của NOAA.
- Từ chối ngay lập tức (`ValueError`) nếu phát hiện: bảng rỗng, ngày bị khuyết/không liên tục, trùng khóa ngày, ngày ENSO không phải thứ Tư, giá trị `NaN`/`inf`, hoặc mã thiếu dữ liệu sentinel của NOAA (`|x| ≥ 99`).

---

## 3. Cách chạy pipeline & kiểm thử

```bash
# 1. Chạy Unit Tests kiểm định parser, validation & logic rolling
python3 silver/scripts/test_silver_pipeline.py -v

# 2. Chạy tái tạo toàn bộ Silver từ Bronze mới nhất
python3 silver/scripts/noaa_bronze_to_silver.py --overwrite

# 3. Kiểm chứng độ tương đồng 100% giữa Silver và Gold climate_weekly
python3 silver/scripts/verify_silver_to_gold.py --weeks 20200107 20210601 20220628
```
