# Hệ thống lưu vết hành vi người dùng trên website TMĐT (Cassandra + Redis)

Tài liệu thiết kế đầy đủ (kiến trúc, thiết kế CSDL): [`docs/THIET_KE.md`](docs/THIET_KE.md). Sơ đồ kiến trúc: `docs/kien_truc.png` / `docs/kien_truc.svg`.

## Cấu trúc thư mục

| File | Vai trò |
|---|---|
| `docker-compose.yml` | Chạy Cassandra 4.1 và Redis 7 |
| `schema.cql` | Keyspace và 4 bảng Cassandra |
| `init_schema.py` | Chạy `schema.cql` bằng Python (không cần cqlsh) |
| `common.py` | Cấu hình, danh mục sản phẩm mẫu, hàm kết nối |
| `simulator.py` | Giả lập website: ghi sự kiện vào Cassandra + Redis (chế độ bulk 1 triệu+ và realtime) |
| `query_gui.py` | **Giao diện đồ họa (Tkinter)** để truy vấn dữ liệu, 7 tab tương ứng 7 chức năng |
| `query_cli.py` | Chương trình truy vấn dạng dòng lệnh (menu); `query_gui.py` dùng lại lớp truy vấn của file này |
| `docs/` | Tài liệu thiết kế và sơ đồ kiến trúc |

## Cách chạy (Windows)

```bat
:: 1. Khởi động CSDL (Cassandra mất khoảng 1-2 phút để healthy)
docker compose up -d
docker compose ps

:: 2. Tạo và kích hoạt môi trường ảo, cài thư viện
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

:: 3. Tạo keyspace và bảng
python init_schema.py

:: 4. Ghi 1.000.000 sự kiện (mặc định: 1000 user, rải 7 ngày)
python simulator.py --reset

:: 5. Truy vấn bằng giao diện đồ họa
python query_gui.py

:: (hoặc bản dòng lệnh)
python query_cli.py
```

Linux/macOS: thay bước kích hoạt bằng `source .venv/bin/activate`.

## Các tuỳ chọn của simulator

```bat
python simulator.py --reset --events 2000000 --users 5000 --days 14   :: lượng dữ liệu lớn hơn
python simulator.py --realtime --delay 0.5                            :: luồng trực tiếp, Ctrl+C để dừng
python simulator.py --reset --events 10000 --users 50                 :: chạy thử nhanh
```

Mỗi sự kiện tạo 2 hàng log (`events_by_user`, `events_by_session`), nên 1.000.000 sự kiện tương ứng 2.000.000 hàng trong Cassandra.
Tốc độ ghi phụ thuộc cấu hình máy và Docker. Nếu gặp lỗi timeout, giảm `--concurrency` (ví dụ 32) hoặc tăng RAM cho Docker.

## Kiểm tra tối thiểu 1.000.000 bản ghi

Trong `python query_cli.py`:
- Chức năng **6**: tổng số sự kiện theo ngày và loại (cộng từ bảng counter, kết quả tức thời).
- Chức năng **7**: đếm chính xác số hàng của `events_by_user` bằng cách đếm từng partition.

Kiểm tra nhanh bằng công cụ gốc:

```bat
docker exec -it clickstream-cassandra cqlsh -e "SELECT * FROM ecommerce_tracking.events_by_user LIMIT 5;"
docker exec -it clickstream-redis redis-cli ZREVRANGE user:U0001:recent_views 0 4 WITHSCORES
docker exec -it clickstream-cassandra nodetool tablestats ecommerce_tracking.events_by_user
```

Lưu ý: `SELECT COUNT(*)` trên toàn bảng hàng triệu hàng thường bị timeout trong Cassandra, nên dùng chức năng 6 hoặc 7.

## Gợi ý demo

1. Chạy `simulator.py --realtime` ở terminal 1.
2. Mở `query_cli.py` ở terminal 2, chọn chức năng 1 với `U0001`... để xem danh sách 5 sản phẩm thay đổi theo thời gian thực.
3. Dùng chức năng 2, 3, 4, 5 để minh họa các truy vấn phân tích.

Mã user giả lập có dạng `U0001` đến `U1000` (theo `--users`).

## Giao diện đồ họa (`query_gui.py`)

Cửa sổ gồm 7 tab: sản phẩm vừa xem (Redis), lịch sử hành vi, hành trình phiên, thống kê sản phẩm, từ khóa tìm kiếm, tổng quan sự kiện, đếm chính xác số hàng.
- Nhấn Enter trong ô nhập hoặc nút **Truy vấn** để chạy; truy vấn chạy nền nên cửa sổ không bị treo.
- Ở tab 2, nhấp đúp vào một dòng để chuyển sang tab 3 và xem hành trình của phiên đó.
- Nhấp vào tiêu đề cột để sắp xếp bảng; tab 4, 5, 6 có biểu đồ cột.
- Nếu báo `No module named tkinter`: chạy lại trình cài Python, chọn Modify và tick **tcl/tk and IDLE**.

## Hai truy vấn bổ sung (chức năng 8 và 9)

Hai truy vấn này được thêm vào `query_cli.py` (menu 8, 9) và `query_gui.py` (tab 8, 9). Chúng chỉ đọc dữ liệu từ các bảng đã có, không thay đổi `schema.cql`, `simulator.py` hay dữ liệu đã ghi.

| Chức năng | Kết quả | Bảng và khóa truy vấn |
|---|---|---|
| 8. Xu hướng tương tác của một sản phẩm theo ngày | View, click, add_to_cart và tỷ lệ giỏ/view của một sản phẩm qua N ngày gần nhất (có biểu đồ cột ở tab 8) | `product_stats_by_day` (`event_date` + `product_id`) |
| 9. Các phiên truy cập của một khách hàng trong ngày | Mỗi phiên: giờ bắt đầu, thời lượng, số sự kiện, số view, số lần thêm giỏ, thiết bị, Session ID | `events_by_user` (`user_id` + `event_date`), gom theo `session_id` |

CQL tương ứng:

```sql
SELECT event_type, cnt FROM product_stats_by_day WHERE event_date = ? AND product_id = ?;
SELECT session_id, event_time, event_type, device FROM events_by_user WHERE user_id = ? AND event_date = ?;
```

Ở chức năng 9, nhấp đúp vào một dòng để chuyển sang chức năng 3 và xem hành trình của phiên đó. Phiên kéo dài qua nửa đêm (giờ VN) chỉ hiện phần thuộc ngày được chọn.

## Xử lý sự cố

- Lỗi `Unable to load a default connection class` trên Python 3.12+: cần `pip install pyasyncore` (đã có trong `requirements.txt`).
- Lỗi chạy bằng nút Run báo `/usr/bin/env`: chạy bằng lệnh `python <file>.py` trong terminal.
- Muốn xóa sạch và làm lại: `python simulator.py --reset ...`, hoặc `docker compose down -v` để xóa luôn volume.
