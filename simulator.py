"""
simulator.py - Giả lập website TMĐT sinh sự kiện clickstream và ghi vào hệ thống.

Hai chế độ:
  1) BULK (mặc định): sinh và ghi hàng loạt --events sự kiện (mặc định 1.000.000) rải trong --days ngày.
     Ghi bất đồng bộ (execute_concurrent), gộp counter theo lô, cập nhật Redis theo lô -> rất nhanh.
  2) REALTIME (--realtime): mô phỏng lưu lượng trực tiếp, mỗi sự kiện ghi ngay (LOGGED BATCH).

Mỗi sự kiện được ghi vào:
  - Cassandra: events_by_user + events_by_session (2 hàng), và bảng counter thống kê
  - Redis    : chỉ với sự kiện 'view' -> giữ 5 sản phẩm xem gần nhất của user

Ví dụ:
  python simulator.py --reset                          # 1.000.000 sự kiện, 1000 user, 7 ngày
  python simulator.py --reset --events 2000000 --users 5000
  python simulator.py --realtime --delay 0.5           # luồng trực tiếp, Ctrl+C để dừng
"""
import argparse
import random
import time
import uuid
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone

from cassandra.concurrent import execute_concurrent
from cassandra.query import UNSET_VALUE, BatchStatement, BatchType

from common import (
    CASSANDRA_TABLES,
    CATEGORIES,
    KEYSPACE,
    KEYWORDS,
    PRODUCTS,
    RECENT_VIEW_LIMIT,
    RECENT_VIEW_TTL,
    connect_cassandra,
    connect_redis,
    recent_views_key,
    to_vn,
)

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [60, 35, 5]

PRODUCT_IDS = list(PRODUCTS)
# Độ phổ biến giảm dần để thống kê "top sản phẩm" có ý nghĩa
POPULARITY = {pid: 1 / (i + 1) ** 0.7 for i, pid in enumerate(PRODUCT_IDS)}
POOL_BY_CATEGORY = {c: [p for p in PRODUCT_IDS if PRODUCTS[p][1] == c] for c in CATEGORIES}


def utcnow():
    return datetime.now(timezone.utc)


def pick_product(category):
    pool = POOL_BY_CATEGORY[category]
    return random.choices(pool, weights=[POPULARITY[p] for p in pool])[0]


def with_retry(fn, attempts=5):
    """Thử lại khi Cassandra quá tải tạm thời (timeout...). Insert là idempotent nên thử lại an toàn."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as err:  # noqa: BLE001
            if i == attempts:
                raise
            wait = 2 * i
            print(f"\n  ! lỗi tạm thời ({type(err).__name__}), thử lại sau {wait}s ({i}/{attempts})")
            time.sleep(wait)


# ---------------------------------------------------------------------------
# 1. Sinh hành vi người dùng
# ---------------------------------------------------------------------------
def generate_session(user_id, start):
    """Sinh danh sách sự kiện của một phiên truy cập.

    Hành trình mẫu: (search | mở trang danh mục) -> click sản phẩm -> view -> (add_to_cart)
    """
    session_id = uuid.uuid4()
    device = random.choices(DEVICES, DEVICE_WEIGHTS)[0]
    events = []
    t = start

    def emit(event_type, url, product_id=None, keyword=None, quantity=None):
        nonlocal t
        t += timedelta(seconds=random.randint(1, 20))
        events.append({
            "event_id": uuid.uuid4(),
            "event_time": t,
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "product_id": product_id,
            "category": PRODUCTS[product_id][1] if product_id else None,
            "search_keyword": keyword,
            "quantity": quantity,
            "page_url": url,
            "device": device,
        })

    for _ in range(random.randint(1, 4)):  # số lượt "tìm - xem" trong một phiên
        if random.random() < 0.5:
            keyword = random.choice(list(KEYWORDS))
            category = KEYWORDS[keyword]
            list_url = "/search?q=" + keyword.replace(" ", "+")
            emit("search", list_url, keyword=keyword)
        else:  # mở trang danh mục (không phát sinh sự kiện riêng)
            category = random.choice(CATEGORIES)
            list_url = f"/category/{category}"

        for _ in range(random.randint(1, 3)):  # xem 1-3 sản phẩm trong danh sách
            pid = pick_product(category)
            emit("click", list_url, product_id=pid)           # click vào thẻ sản phẩm
            emit("view", f"/product/{pid}", product_id=pid)   # mở trang chi tiết
            if random.random() < 0.25:
                emit("add_to_cart", f"/product/{pid}", product_id=pid,
                     quantity=random.randint(1, 2))
    return events


# ---------------------------------------------------------------------------
# 2. Ghi dữ liệu vào Cassandra + Redis
# ---------------------------------------------------------------------------
class Writer:
    COLUMNS = ("user_id, event_date, event_time, event_id, session_id, event_type, "
               "product_id, category, search_keyword, quantity, page_url, device")

    def __init__(self, cass, rds):
        self.cass = cass
        self.rds = rds
        self.count = 0  # số sự kiện đã ghi

        marks = ", ".join(["?"] * len(self.COLUMNS.split(",")))
        self.ins_user = cass.prepare(
            f"INSERT INTO events_by_user ({self.COLUMNS}) VALUES ({marks})")
        self.ins_session = cass.prepare(
            f"INSERT INTO events_by_session ({self.COLUMNS}) VALUES ({marks})")
        # Counter nhận số gia tăng n để có thể cộng dồn cả lô trong một lệnh
        self.cnt_product = cass.prepare(
            "UPDATE product_stats_by_day SET cnt = cnt + ? "
            "WHERE event_date = ? AND product_id = ? AND event_type = ?")
        self.cnt_keyword = cass.prepare(
            "UPDATE keyword_stats_by_day SET cnt = cnt + ? "
            "WHERE event_date = ? AND search_keyword = ?")

    @staticmethod
    def _params(ev):
        # Trường rỗng dùng UNSET_VALUE thay vì None để không sinh tombstone trong Cassandra
        def v(x):
            return UNSET_VALUE if x is None else x

        return (
            ev["user_id"],
            to_vn(ev["event_time"]).date(),     # event_date: bucket theo ngày giờ VN
            ev["event_time"],
            ev["event_id"],
            ev["session_id"],
            ev["event_type"],
            v(ev["product_id"]),
            v(ev["category"]),
            v(ev["search_keyword"]),
            v(ev["quantity"]),
            ev["page_url"],
            ev["device"],
        )

    # ----- Ghi từng sự kiện (realtime) -----
    def write(self, ev):
        params = self._params(ev)
        day = params[1]

        # (1) Ghi trùng vào 2 bảng log bằng LOGGED BATCH để 2 bảng luôn nhất quán
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self.ins_user, params)
        batch.add(self.ins_session, params)
        self.cass.execute(batch)

        # (2) Bảng thống kê (counter không được trộn chung batch với bảng thường)
        if ev["product_id"]:
            self.cass.execute(self.cnt_product, (1, day, ev["product_id"], ev["event_type"]))
        if ev["event_type"] == "search":
            self.cass.execute(self.cnt_keyword, (1, day, ev["search_keyword"]))

        # (3) Redis: chỉ sự kiện 'view'
        if ev["event_type"] == "view":
            score = int(ev["event_time"].timestamp() * 1000)
            self._update_recent({ev["user_id"]: {ev["product_id"]: score}})

        self.count += 1

    # ----- Ghi theo lô (bulk) -----
    def write_chunk(self, events, concurrency=64):
        """Ghi một lô sự kiện: insert bất đồng bộ + gộp counter + cập nhật Redis một lần."""
        statements = []
        product_cnt = Counter()
        keyword_cnt = Counter()
        recent = defaultdict(dict)  # user_id -> {product_id: score lớn nhất trong lô}

        for ev in events:
            params = self._params(ev)
            day = params[1]
            statements.append((self.ins_user, params))
            statements.append((self.ins_session, params))
            if ev["product_id"]:
                product_cnt[(day, ev["product_id"], ev["event_type"])] += 1
            if ev["event_type"] == "search":
                keyword_cnt[(day, ev["search_keyword"])] += 1
            if ev["event_type"] == "view":
                score = int(ev["event_time"].timestamp() * 1000)
                views = recent[ev["user_id"]]
                if score > views.get(ev["product_id"], 0):
                    views[ev["product_id"]] = score

        # Insert idempotent (cùng khóa chính, cùng giá trị) nên thử lại an toàn
        with_retry(lambda: execute_concurrent(
            self.cass, statements, concurrency=concurrency, raise_on_first_error=True))

        # Counter: cộng dồn cả lô -> chỉ vài chục lệnh thay vì hàng nghìn lệnh "+1"
        # (Counter không idempotent: nếu timeout mơ hồ rồi thử lại có thể lệch rất nhỏ.)
        for (day, pid, etype), n in product_cnt.items():
            with_retry(lambda: self.cass.execute(self.cnt_product, (n, day, pid, etype)))
        for (day, kw), n in keyword_cnt.items():
            with_retry(lambda: self.cass.execute(self.cnt_keyword, (n, day, kw)))

        self._update_recent(recent)
        self.count += len(events)

    # ----- Redis: 5 sản phẩm xem gần nhất -----
    def _update_recent(self, recent_by_user):
        pipe = self.rds.pipeline(transaction=False)
        for user_id, views in recent_by_user.items():
            key = recent_views_key(user_id)
            top = dict(sorted(views.items(), key=lambda kv: kv[1], reverse=True)[:RECENT_VIEW_LIMIT])
            # GT: chỉ cập nhật score nếu mới hơn (an toàn khi dữ liệu đến trễ / ghi lô)
            pipe.zadd(key, top, gt=True)
            # Cắt bớt, chỉ giữ RECENT_VIEW_LIMIT phần tử có score lớn nhất
            pipe.zremrangebyrank(key, 0, -(RECENT_VIEW_LIMIT + 1))
            pipe.expire(key, RECENT_VIEW_TTL)
        pipe.execute()


# ---------------------------------------------------------------------------
# 3. Hai chế độ chạy
# ---------------------------------------------------------------------------
def run_bulk(writer, users, total, days, chunk, concurrency):
    """Sinh và ghi đúng `total` sự kiện, rải đều trong `days` ngày gần nhất."""
    now = utcnow()
    buffer, produced = [], 0
    started = time.time()

    while produced < total:
        start = now - timedelta(seconds=random.uniform(1800, days * 86400))
        events = generate_session(random.choice(users), start)[: total - produced]
        buffer.extend(events)
        produced += len(events)

        if len(buffer) >= chunk or produced >= total:
            writer.write_chunk(buffer, concurrency)
            buffer = []
            elapsed = max(time.time() - started, 1e-6)
            rate = writer.count / elapsed
            eta = (total - writer.count) / rate if rate else 0
            print(f"  {writer.count:>10,}/{total:,} sự kiện ({writer.count / total:6.1%}) "
                  f"| {rate:,.0f} sự kiện/s | còn khoảng {eta / 60:4.1f} phút", end="\r", flush=True)
    print()


def log_event(ev):
    when = to_vn(ev["event_time"]).strftime("%H:%M:%S")
    detail = ""
    if ev["product_id"]:
        detail = f"{ev['product_id']} {PRODUCTS[ev['product_id']][0]}"
    elif ev["search_keyword"]:
        detail = f"\"{ev['search_keyword']}\""
    print(f"[{when}] {ev['user_id']}  {ev['event_type']:<12} {detail}")


def run_realtime(writer, users, n_sessions, delay, active_sessions):
    """Giả lập lưu lượng trực tiếp: nhiều phiên diễn ra xen kẽ, timestamp = thời điểm thật."""
    limit = n_sessions if n_sessions > 0 else float("inf")
    active, started = [], 0
    while started < limit or active:
        while started < limit and len(active) < active_sessions:
            active.append(deque(generate_session(random.choice(users), utcnow())))
            started += 1
        queue = random.choice(active)
        ev = queue.popleft()
        ev["event_time"] = utcnow()
        writer.write(ev)
        log_event(ev)
        if not queue:
            active.remove(queue)
        time.sleep(random.uniform(0, delay))


def reset_all(cass, rds):
    for table in CASSANDRA_TABLES:
        cass.execute(f"TRUNCATE {table}")
    for key in rds.scan_iter("user:*:recent_views"):
        rds.delete(key)
    print("Đã xóa dữ liệu cũ trong Cassandra và Redis.")


def main():
    parser = argparse.ArgumentParser(description="Giả lập ghi dữ liệu clickstream TMĐT")
    parser.add_argument("--events", type=int, default=1_000_000,
                        help="(bulk) tổng số sự kiện cần ghi, mặc định 1.000.000")
    parser.add_argument("--users", type=int, default=1000, help="số người dùng giả lập (tối đa 9999)")
    parser.add_argument("--days", type=int, default=7, help="(bulk) rải dữ liệu trong N ngày gần nhất")
    parser.add_argument("--chunk", type=int, default=5000, help="(bulk) số sự kiện mỗi lô ghi")
    parser.add_argument("--concurrency", type=int, default=64,
                        help="(bulk) số lệnh ghi chạy song song tới Cassandra")
    parser.add_argument("--realtime", action="store_true", help="ghi theo thời gian thực")
    parser.add_argument("--sessions", type=int, default=0,
                        help="(realtime) số phiên truy cập, 0 = chạy mãi")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="(realtime) độ trễ tối đa giữa 2 sự kiện (giây)")
    parser.add_argument("--active-sessions", type=int, default=5,
                        help="(realtime) số phiên diễn ra đồng thời")
    parser.add_argument("--reset", action="store_true", help="xóa sạch dữ liệu cũ trước khi chạy")
    parser.add_argument("--seed", type=int, help="seed để lặp lại cùng một kịch bản")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    cass = connect_cassandra(KEYSPACE)
    rds = connect_redis()
    users = [f"U{i:04d}" for i in range(1, args.users + 1)]
    started_at = time.time()
    writer = None

    try:
        if args.reset:
            reset_all(cass, rds)
        writer = Writer(cass, rds)
        if args.realtime:
            print(f"Giả lập REALTIME với {len(users)} người dùng (Ctrl+C để dừng)...")
            run_realtime(writer, users, args.sessions, args.delay, args.active_sessions)
        else:
            print(f"Giả lập BULK: {args.events:,} sự kiện, {len(users)} người dùng, "
                  f"rải trong {args.days} ngày...")
            run_bulk(writer, users, args.events, args.days, args.chunk, args.concurrency)
    except KeyboardInterrupt:
        print("\nĐã dừng theo yêu cầu.")
    finally:
        if writer:
            elapsed = max(time.time() - started_at, 1e-6)
            print(f"Đã ghi {writer.count:,} sự kiện ({writer.count * 2:,} hàng log trong Cassandra) "
                  f"trong {elapsed:.1f}s ({writer.count / elapsed:,.0f} sự kiện/giây).")
        cass.cluster.shutdown()


if __name__ == "__main__":
    main()
