# References — NOAA Teleconnections

Thư mục này ghi nhận đầy đủ các nguồn khoa học, mã nguồn gốc và dữ liệu chính thức làm cơ sở xây dựng pipeline **NOAA Teleconnections (Bronze → Silver → Gold)**.

## 1. Bài báo & Bộ dữ liệu gốc
- **Paper**: Erhardt, R., Di Vittorio, C. A., Hepler, S. A., Lowman, L. E. L., & Wei, W. (2024). *Homogenized gridded dataset for drought and hydrometeorological modeling for the continental United States*. Scientific Data, 11, 375. https://doi.org/10.1038/s41597-024-03202-6
- **Dryad Official Dataset**: https://datadryad.org/dataset/doi:10.5061/dryad.g1jwstqw7
- **Author Repository**: https://github.com/heplersa/USDMdata
- **Reference Script**: `TeleconnectionsForDryad.R` (xử lý AO, NAO, PNA hằng ngày và ENSO Niño 3.4 hằng tuần thành các cột `ao`, `nao`, `pna`, `enso`, `enso14`, `enso28`, `enso84` căn chỉnh với thứ Ba của USDM).

## 2. Nguồn dữ liệu chính thức NOAA / CPC
- **AO Daily Index (CDAS z1000)**: https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.ao.cdas.z1000.19500101_current.csv
- **NAO Daily Index (CDAS z500)**: https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.nao.cdas.z500.19500101_current.csv
- **PNA Daily Index (CDAS z500)**: https://ftp.cpc.ncep.noaa.gov/cwlinks/norm.daily.pna.cdas.z500.19500101_current.csv
- **ENSO Weekly SST & SSTA (1991–2020 base)**: https://www.cpc.ncep.noaa.gov/data/indices/wksst9120.for

## 3. Đối chiếu với `TeleconnectionsForDryad.R`
| Khía cạnh | Script gốc (`TeleconnectionsForDryad.R`) | Pipeline `NOAA_KienNT` |
| :--- | :--- | :--- |
| Kiến trúc lưu trữ | Đọc thẳng web → xuất CSV Gold | Tách 3 tầng Medallion: Bronze (raw + SHA-256) → Silver (Parquet) → Gold (Parquet + CSV) |
| Đọc `wksst9120.for` | `read.fwf(..., widths=...)`, lấy cột Niño 3.4 SSTA | Parser regex/fixed-width tách an toàn âm dính nhau (`25.7-0.9`), giữ cả SST & SSTA ở Silver |
| AO/NAO/PNA tuần | `rowMeans` 7 ngày kết thúc thứ Ba (`na.rm=TRUE`) | `rolling(7, min_periods=7).mean()` kết thúc thứ Ba (chặn cửa sổ thiếu) |
| ENSO & lag | Thứ Tư dịch về thứ Ba (`-1 ngày`), tính `enso`, `enso14`, `enso28`, `enso84` | Tương đương hoàn toàn, làm tròn 2 chữ số (`ao/nao/pna/enso`) & 3 chữ số (`enso14/28/84`) |
| Kiểm chứng | Không có unit test tự động | Có `test_silver_pipeline.py`, `verify_silver_to_gold.py`, `quality_report.json`, `manifest.json` |
