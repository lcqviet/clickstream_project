# Hệ thống lưu vết hành vi người dùng trên website TMĐT (Cassandra + Redis)

## Cấu trúc thư mục

| File | Vai trò |
|---|---|
| `docker-compose.yml` | Chạy Cassandra 4.1 và Redis 7 |
| `schema.cql` | Keyspace và 4 bảng Cassandra |
| `init_schema.py` | Chạy `schema.cql` bằng Python (không cần cqlsh) |
| `common.py` | Cấu hình, danh mục sản phẩm mẫu, hàm kết nối |
| `simulator.py` | Giả lập website: sinh và ghi sự kiện vào Cassandra + Redis |
| `query_cli.py` | Chương trình truy vấn dữ liệu (menu tương tác) |

## Cách chạy

```bash
# 1. Khởi động CSDL (Cassandra mất khoảng 1-2 phút để sẵn sàng)
docker compose up -d
docker compose ps            # đợi cassandra ở trạng thái "healthy"

# 2. Cài thư viện Python (khuyến nghị Python 3.9 - 3.12)
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Tạo keyspace và bảng
python init_schema.py

# 4. Ghi dữ liệu giả lập
python simulator.py --reset --users 50 --sessions 300 --days 7     # dữ liệu lịch sử 7 ngày
python simulator.py --realtime --sessions 0 --delay 0.5            # (tuỳ chọn) luồng trực tiếp, Ctrl+C để dừng

# 5. Truy vấn
python query_cli.py
```

Mã user giả lập có dạng `U0001` ... `U0050`.

## Kiểm tra nhanh bằng công cụ gốc

```bash
docker exec -it clickstream-cassandra cqlsh -e "SELECT * FROM ecommerce_tracking.events_by_user LIMIT 5;"
docker exec -it clickstream-redis redis-cli ZREVRANGE user:U0001:recent_views 0 4 WITHSCORES
```

## Tóm tắt thiết kế

- **Redis**: Sorted Set `user:{id}:recent_views`, member = `product_id`, score = timestamp (ms).
  Mỗi sự kiện `view`: `ZADD GT` -> `ZREMRANGEBYRANK key 0 -6` -> `EXPIRE 30 ngày`. Đọc bằng `ZREVRANGE key 0 4`.
- **Cassandra**:
  - `events_by_user`: partition `(user_id, event_date)`, clustering `event_time DESC`, lịch sử theo user.
  - `events_by_session`: partition `session_id`, clustering `event_time ASC`, hành trình một phiên.
  - `product_stats_by_day`, `keyword_stats_by_day`: bảng counter phục vụ thống kê.
- Ngày (`event_date`) tính theo giờ Việt Nam (UTC+7), timestamp lưu UTC.

## Lưu ý và hướng mở rộng

- `SimpleStrategy` với `replication_factor = 1` chỉ để chạy thử trên một node. Triển khai thật dùng `NetworkTopologyStrategy`, RF = 3.
- Bảng counter có partition theo ngày nên các lượt ghi cùng ngày dồn vào một partition (hot partition khi traffic rất lớn).
  Có thể chia nhỏ thêm theo giờ hoặc theo shard, hoặc dùng Spark để tổng hợp từ `events_by_user`.
- Có thể dùng `TimeWindowCompactionStrategy` cho bảng log và đặt `default_time_to_live` nếu chỉ cần giữ log trong một khoảng thời gian.
- Trong thực tế, simulator được thay bằng một API thu thập sự kiện (hoặc Kafka) đứng giữa website và CSDL.
- Nếu gặp lỗi liên quan `asyncore` trên Python 3.12+, hãy nâng `cassandra-driver` lên bản mới nhất hoặc dùng Python 3.11.
