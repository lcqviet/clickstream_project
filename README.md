# Hệ thống lưu vết hành vi người dùng trên website TMĐT (Cassandra + Redis)


## Cấu trúc thư mục

| File | Vai trò |
|---|---|
| `docker-compose.yml` | Chạy Cassandra 4.1 và Redis 7 |
| `schema.cql` | Keyspace và 4 bảng Cassandra |
| `init_schema.py` | Chạy `schema.cql` bằng Python (không cần cqlsh) |
| `common.py` | Cấu hình, danh mục sản phẩm mẫu, hàm kết nối |
| `simulator.py` | Giả lập website: ghi sự kiện vào Cassandra + Redis (chế độ bulk 1 triệu+ và realtime) |
| `query_gui.py` | **Giao diện đồ họa (Tkinter)** để truy vấn dữ liệu, 7 tab tương ứng 7 chức năng |


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

:: 4. Giả lập ghi sự kiện
python simulator.py

:: 5. Truy vấn bằng giao diện đồ họa
python query_gui.py

```

Linux/macOS: thay bước kích hoạt bằng `source .venv/bin/activate`.


Kiểm tra nhanh bằng công cụ gốc:

```bat
docker exec -it clickstream-cassandra cqlsh -e "SELECT * FROM ecommerce_tracking.events_by_user LIMIT 5;"
docker exec -it clickstream-redis redis-cli ZREVRANGE user:U0001:recent_views 0 4 WITHSCORES
docker exec -it clickstream-cassandra nodetool tablestats ecommerce_tracking.events_by_user
```

## demo

1. Chạy `simulator.py` ở terminal 1.
2. Mở `query_gui.py` ở terminal 2, chọn chức năng 1 với `U0001`... để xem danh sách 5 sản phẩm thay đổi theo thời gian thực.

Mã user giả lập có dạng `U0001` đến `U1000` (theo `--users`).

