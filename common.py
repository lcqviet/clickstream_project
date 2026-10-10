"""common.py - Cấu hình, danh mục sản phẩm mẫu và hàm kết nối dùng chung."""
import os
import time
from datetime import datetime, timedelta, timezone

import redis
from cassandra.cluster import Cluster, NoHostAvailable

# ---------------------------------------------------------------------------
# Cấu hình kết nối (có thể ghi đè bằng biến môi trường)
# ---------------------------------------------------------------------------
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = "ecommerce_tracking"

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CASSANDRA_TABLES = [
    "events_by_user",
    "events_by_session",
    "product_stats_by_day",
    "keyword_stats_by_day",
]

# ---------------------------------------------------------------------------
# Quy ước Redis: Sorted Set  user:{user_id}:recent_views
#   member = product_id, score = timestamp (ms) của lần xem gần nhất
# ---------------------------------------------------------------------------
RECENT_VIEW_LIMIT = 5                 # chỉ giữ 5 sản phẩm xem gần nhất
RECENT_VIEW_TTL = 30 * 24 * 3600      # key tự xóa sau 30 ngày không hoạt động


def recent_views_key(user_id: str) -> str:
    return f"user:{user_id}:recent_views"


# ---------------------------------------------------------------------------
# Múi giờ: dữ liệu lưu timestamp UTC, nhưng "ngày" (event_date) tính theo giờ VN
# ---------------------------------------------------------------------------
VN_TZ = timezone(timedelta(hours=7))


def to_vn(dt: datetime) -> datetime:
    """Đổi datetime (naive = UTC, như driver Cassandra trả về) sang giờ Việt Nam."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(VN_TZ)


def today_vn():
    return datetime.now(VN_TZ).date()


# ---------------------------------------------------------------------------
# Danh mục sản phẩm / từ khóa mẫu (dùng cho giả lập và hiển thị tên sản phẩm)
# ---------------------------------------------------------------------------
PRODUCTS = {
    "P001": ("iPhone 15 128GB", "dien-thoai"),
    "P002": ("Samsung Galaxy S24", "dien-thoai"),
    "P003": ("Xiaomi Redmi Note 13", "dien-thoai"),
    "P004": ("Laptop Dell Inspiron 15", "laptop"),
    "P005": ("MacBook Air M2", "laptop"),
    "P006": ("Laptop Asus Vivobook 14", "laptop"),
    "P007": ("Tai nghe AirPods Pro 2", "phu-kien"),
    "P008": ("Tai nghe Sony WH-1000XM5", "phu-kien"),
    "P009": ("Sạc dự phòng Anker 20000mAh", "phu-kien"),
    "P010": ("Giày chạy bộ Nike Pegasus", "thoi-trang"),
    "P011": ("Giày Adidas Ultraboost", "thoi-trang"),
    "P012": ("Áo thun Uniqlo Cotton", "thoi-trang"),
    "P013": ("Nồi chiên không dầu Philips", "gia-dung"),
    "P014": ("Máy hút bụi cầm tay Xiaomi", "gia-dung"),
    "P015": ("Robot hút bụi Ecovacs", "gia-dung"),
}

KEYWORDS = {
    "iphone": "dien-thoai",
    "samsung": "dien-thoai",
    "laptop": "laptop",
    "macbook": "laptop",
    "tai nghe": "phu-kien",
    "sạc dự phòng": "phu-kien",
    "giày chạy bộ": "thoi-trang",
    "áo thun": "thoi-trang",
    "nồi chiên không dầu": "gia-dung",
    "robot hút bụi": "gia-dung",
}

CATEGORIES = sorted({cat for _, cat in PRODUCTS.values()})


def product_name(product_id: str) -> str:
    return PRODUCTS.get(product_id, ("(không rõ)", ""))[0]


# ---------------------------------------------------------------------------
# Hàm kết nối
# ---------------------------------------------------------------------------
def connect_cassandra(keyspace=None, retries=20, wait=3):
    """Kết nối Cassandra, tự thử lại vì container khởi động khá chậm."""
    last_err = None
    for attempt in range(1, retries + 1):
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        try:
            return cluster.connect(keyspace)
        except NoHostAvailable as err:
            cluster.shutdown()
            last_err = err
            print(f"[Cassandra] chưa sẵn sàng (lần {attempt}/{retries}), thử lại sau {wait}s...")
            time.sleep(wait)
    raise SystemExit(f"Không kết nối được Cassandra: {last_err}")


def connect_redis():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        client.ping()
    except redis.exceptions.ConnectionError as err:
        raise SystemExit(f"Không kết nối được Redis ({REDIS_HOST}:{REDIS_PORT}): {err}")
    return client
