# Gold Layer (Curated / ML-Ready Weekly Climate Mart)
## NOAA Teleconnections (2020 – 2022, USDM-Aligned)

Tầng dữ liệu tinh luyện (**Curated Mart**) tổng hợp các chỉ số viễn liên khí hậu theo đúng quy tắc của **Erhardt et al. (2024)** (`TeleconnectionsForDryad.R`), đồng bộ hoàn toàn với 130 tuần thứ Ba của U.S. Drought Monitor (07/01/2020 – 28/06/2022), sẵn sàng ghép vào bảng lưới không gian – thời gian cho mô hình dự báo hạn hán.

---

## 1. Cấu trúc thư mục

```text
gold/
├── data/
│   ├── climate_weekly.parquet              # Bảng tuần chuẩn hóa (130 dòng × 8 cột, Snappy)
│   ├── climate_weekly.csv                  # Bản xuất CSV phẳng tương đương
│   ├── usdm_week_dates.csv                 # Lịch 130 ngày thứ Ba tham chiếu từ USDM Gold
│   ├── validation.txt                      # Báo cáo kiểm định & thống kê mô tả
│   └── comparison_reports/
│       ├── weekly_lineage.csv              # Bảng truy vết 130 tuần (week_date <-> cửa sổ ngày & thứ Tư ENSO)
│       └── verification_summary.json       # Kết quả đối sánh độc lập Silver -> Gold -> USDM
├── scripts/
│   ├── noaa_silver_to_gold.py              # Pipeline tổng hợp Silver -> Gold
│   ├── input_manifest.csv                  # Bảng kê tệp đầu vào & SHA-256
│   └── environment_info.txt                # Nhật ký phiên bản Python, pandas, numpy, pyarrow
└── README.md
```

---

## 2. Đặc tả dữ liệu & Quy tắc tổng hợp

- **Kích thước bảng `climate_weekly`**: **130 dòng × 8 cột** (`2020-01-07` → `2022-06-28`), 100% là thứ Ba, 0 null, 0 trùng lặp.
- **Khóa chính**: `week_date` (khớp 100% với cột `time` trong `USDM_PQA_draftrepo/gold/data/usdmfinal.csv`).

| Cột | Kiểu dữ liệu | Quy tắc tính toán (theo `TeleconnectionsForDryad.R`) | Làm tròn |
| :--- | :--- | :--- | :---: |
| `week_date` | `datetime64[ns]` | Ngày thứ Ba hiệu lực của kỳ USDM (`YYYY-MM-DD`) | — |
| `ao` | `float64` | Trung bình trượt 7 ngày của AO kết thúc vào `week_date` (`[t-6, t]`) | 2 chữ số |
| `nao` | `float64` | Trung bình trượt 7 ngày của NAO kết thúc vào `week_date` (`[t-6, t]`) | 2 chữ số |
| `pna` | `float64` | Trung bình trượt 7 ngày của PNA kết thúc vào `week_date` (`[t-6, t]`) | 2 chữ số |
| `enso` | `float64` | Giá trị tuần `nino34_ssta` tại thứ Tư liền sau (`t + 1 ngày`) | 2 chữ số |
| `enso14` | `float64` | Trung bình trượt 2 tuần (14 ngày) của `nino34_ssta` kết thúc tại `t + 1 ngày` | 3 chữ số |
| `enso28` | `float64` | Trung bình trượt 4 tuần (28 ngày) của `nino34_ssta` kết thúc tại `t + 1 ngày` | 3 chữ số |
| `enso84` | `float64` | Trung bình trượt 12 tuần (84 ngày) của `nino34_ssta` kết thúc tại `t + 1 ngày` | 3 chữ số |

### Lưu ý về kiểm định nghiêm ngặt & truy vết
- **Cửa sổ đầy đủ (`min_periods=window`)**: Khác với `na.rm=TRUE` trong R gốc, pipeline Python yêu cầu đủ 100% quan sát trong từng cửa sổ (`7 ngày` và `1/2/4/12 tuần`) và dừng lỗi ngay nếu khuyết dữ liệu.
- **Truy vết ngày gốc**: Toàn bộ ánh xạ từ `week_date` (thứ Ba) sang `daily_window_start..end` và `enso_source_week_date` (thứ Tư gốc) được lưu rõ trong `gold/data/comparison_reports/weekly_lineage.csv`.
- **Cảnh báo dự báo thời gian thực**: Cách căn chỉnh thứ Tư → thứ Ba tái hiện chính xác bộ dữ liệu hồi cứu (retrospective) của bài báo gốc; nếu triển khai dự báo thực tế theo thời gian thực cần kiểm tra độ trễ công bố của NOAA CPC để tránh rò rỉ thông tin tương lai.

---

## 3. Cách chạy tái tạo Gold Layer

```bash
# Chạy với lịch tuần usdm_week_dates.csv có sẵn trong repo
python3 gold/scripts/noaa_silver_to_gold.py --overwrite

# Hoặc đồng bộ trực tiếp lịch tuần từ file USDM Gold usdmfinal.csv
python3 gold/scripts/noaa_silver_to_gold.py --usdm-csv ../USDM_PQA_draftrepo/gold/data/usdmfinal.csv --overwrite
```
