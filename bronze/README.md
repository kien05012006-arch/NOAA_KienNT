# Bronze Layer (Raw NOAA/CPC Teleconnections Data)
## NOAA Climate Prediction Center (Buffered 2019 – 2022)

Tầng lưu trữ dữ liệu thô (Raw Ingestion) được thu thập trực tiếp từ cổng dữ liệu công khai của **NOAA Climate Prediction Center (CPC)** theo phân vùng ngày nạp (`ingest_date=YYYY-MM-DD`), đảm bảo tính bất biến và truy xuất nguồn gốc qua mã băm SHA-256.

---

## 1. Cấu trúc thư mục

```text
bronze/
├── data/
│   ├── ingest_date=2026-10-17/
│   │   ├── norm.daily.ao.cdas.z1000.19500101_current.csv   # Chỉ số AO ngày (1.461 dòng)
│   │   ├── norm.daily.nao.cdas.z500.19500101_current.csv   # Chỉ số NAO ngày (1.461 dòng)
│   │   ├── norm.daily.pna.cdas.z500.19500101_current.csv   # Chỉ số PNA ngày (1.461 dòng)
│   │   ├── wksst9120.for                                   # ENSO Niño 3.4 tuần, fixed-width (209 dòng)
│   │   └── _manifest.json                                  # Metadata, URL, SHA-256 gốc & đã lưu
│   └── manifest_download.csv                               # Bảng kê trạng thái tải về
├── scripts/
│   ├── crawl_noaa_cpc.py                                   # Crawler tự động tải 4 nguồn NOAA/CPC
│   └── inspect_data.py                                     # Kiểm định SHA-256, dòng/cột, phạm vi ngày
└── README.md
```

---

## 2. Đặc tả nguồn dữ liệu

| Chỉ số | Tệp nguồn NOAA/CPC | Tần suất | Đơn vị & Mốc chuẩn | Phạm vi lưu trữ (Buffer) |
| :--- | :--- | :--- | :--- | :--- |
| **AO** | `norm.daily.ao.cdas.z1000.19500101_current.csv` | Hằng ngày | Chỉ số chuẩn hóa (CDAS z1000, không đơn vị) | 2019-01-01 .. 2022-12-31 (1.461 dòng) |
| **NAO** | `norm.daily.nao.cdas.z500.19500101_current.csv` | Hằng ngày | Chỉ số chuẩn hóa (CDAS z500, không đơn vị) | 2019-01-01 .. 2022-12-31 (1.461 dòng) |
| **PNA** | `norm.daily.pna.cdas.z500.19500101_current.csv` | Hằng ngày | Chỉ số chuẩn hóa (CDAS z500, không đơn vị) | 2019-01-01 .. 2022-12-31 (1.461 dòng) |
| **ENSO (Niño 3.4)** | `wksst9120.for` | Hằng tuần (Thứ Tư) | `SST` & `SSTA` (°C), mốc khí hậu **1991–2020** | 2019-01-02 .. 2022-12-28 (209 dòng) |

### Lưu ý kỹ thuật
- **Chọn chuỗi lịch sử `cdas`**: Không sử dụng tệp dự báo `gfs`/`gefs`.
- **Giữ vùng đệm năm 2019**: Cần thiết để tính trung bình trượt 7 ngày (`AO/NAO/PNA`) và 12 tuần (`enso84`) cho tuần đầu tiên `2020-01-07` mà không bị khuyết dữ liệu biên.
- **Định dạng cố định `.for`**: Tệp `wksst9120.for` có các giá trị âm dính liền cột trước (ví dụ `25.7-0.9`), phải đọc bằng regex / fixed-width ở tầng Silver, tuyệt đối không tách bằng khoảng trắng đơn thuần.
- **Truy vết toàn vẹn**: `_manifest.json` lưu cả `source_sha256` (tệp gốc tải từ NOAA trước khi cắt vùng thời gian) và `saved_sha256` (tệp lưu trong snapshot).

---

## 3. Cách chạy crawler & kiểm tra dữ liệu

```bash
# Kiểm tra toàn vẹn SHA-256 và cấu trúc snapshot hiện có
python3 bronze/scripts/inspect_data.py

# Tải snapshot mới theo ngày hiện tại (idempotent, không ghi đè trừ khi truyền --overwrite)
python3 bronze/scripts/crawl_noaa_cpc.py --start-date 2019-01-01 --end-date 2022-12-31

# Tùy chọn giữ nguyên toàn bộ lịch sử gốc từ NOAA (không cắt ngày/cột)
python3 bronze/scripts/crawl_noaa_cpc.py --keep-raw-full --overwrite
```
