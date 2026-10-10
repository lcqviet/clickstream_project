#!/usr/bin/env python3
"""
simulator.py - Giả lập website TMĐT sinh sự kiện clickstream và ghi vào hệ thống.

Mỗi sự kiện được ghi vào:
  - Cassandra: events_by_user + events_by_session (LOGGED BATCH) và các bảng counter thống kê
  - Redis    : chỉ với sự kiện 'view' -> cập nhật danh sách 5 sản phẩm xem gần nhất

Ví dụ:
  python simulator.py --reset --users 50 --sessions 300 --days 7   # sinh dữ liệu lịch sử nhanh
  python simulator.py --realtime --sessions 0 --delay 0.5          # luồng trực tiếp, Ctrl+C để dừng
"""
import argparse
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

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


def utcnow():
    return datetime.now(timezone.utc)


def pick_product(category):
    pool = [p for p in PRODUCT_IDS if PRODUCTS[p][1] == category]
    return random.choices(pool, weights=[POPULARITY[p] for p in pool])[0]


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
        self.count = 0

        marks = ", ".join(["?"] * len(self.COLUMNS.split(",")))
        self.ins_user = cass.prepare(
            f"INSERT INTO events_by_user ({self.COLUMNS}) VALUES ({marks})")
        self.ins_session = cass.prepare(
            f"INSERT INTO events_by_session ({self.COLUMNS}) VALUES ({marks})")
        self.cnt_product = cass.prepare(
            "UPDATE product_stats_by_day SET cnt = cnt + 1 "
            "WHERE event_date = ? AND product_id = ? AND event_type = ?")
        self.cnt_keyword = cass.prepare(
            "UPDATE keyword_stats_by_day SET cnt = cnt + 1 "
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

    def write(self, ev):
        params = self._params(ev)
        day = params[1]

        # (1) Log chi tiết: ghi trùng vào 2 bảng, dùng LOGGED BATCH để 2 bảng luôn nhất quán
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self.ins_user, params)
        batch.add(self.ins_session, params)
        self.cass.execute(batch)

        # (2) Bảng thống kê (counter không được trộn chung batch với bảng thường)
        if ev["product_id"]:
            self.cass.execute(self.cnt_product, (day, ev["product_id"], ev["event_type"]))
        if ev["event_type"] == "search":
            self.cass.execute(self.cnt_keyword, (day, ev["search_keyword"]))

        # (3) Redis: chỉ sự kiện 'view' -> giữ 5 sản phẩm xem gần nhất
        if ev["event_type"] == "view":
            key = recent_views_key(ev["user_id"])
            score = int(ev["event_time"].timestamp() * 1000)
            pipe = self.rds.pipeline(transaction=True)
            # GT: chỉ cập nhật score nếu mới hơn (an toàn khi dữ liệu đến trễ / backfill)
            pipe.zadd(key, {ev["product_id"]: score}, gt=True)
            # Cắt bớt, chỉ giữ RECENT_VIEW_LIMIT phần tử có score lớn nhất
            pipe.zremrangebyrank(key, 0, -(RECENT_VIEW_LIMIT + 1))
            pipe.expire(key, RECENT_VIEW_TTL)
            pipe.execute()

        self.count += 1


# ---------------------------------------------------------------------------
# 3. Hai chế độ chạy
# ---------------------------------------------------------------------------
def log_event(ev):
    when = to_vn(ev["event_time"]).strftime("%H:%M:%S")
    detail = ""
    if ev["product_id"]:
        detail = f"{ev['product_id']} {PRODUCTS[ev['product_id']][0]}"
    elif ev["search_keyword"]:
        detail = f"\"{ev['search_keyword']}\""
    print(f"[{when}] {ev['user_id']}  {ev['event_type']:<12} {detail}")


def run_backfill(writer, users, n_sessions, days):
    """Sinh dữ liệu lịch sử: rải các phiên đều trong N ngày gần nhất, ghi nhanh không chờ."""
    now = utcnow()
    for i in range(1, n_sessions + 1):
        start = now - timedelta(seconds=random.uniform(600, days * 86400))
        for ev in generate_session(random.choice(users), start):
            writer.write(ev)
        if i % 50 == 0 or i == n_sessions:
            print(f"  ... {i}/{n_sessions} phiên, {writer.count} sự kiện")


def run_realtime(writer, users, n_sessions, delay, concurrent):
    """Giả lập lưu lượng trực tiếp: nhiều phiên diễn ra xen kẽ, timestamp = thời điểm thật."""
    limit = n_sessions if n_sessions > 0 else float("inf")
    active, started = [], 0
    while started < limit or active:
        while started < limit and len(active) < concurrent:
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
    parser.add_argument("--users", type=int, default=50, help="số người dùng giả lập")
    parser.add_argument("--sessions", type=int, default=300,
                        help="số phiên truy cập (chế độ realtime: 0 = chạy mãi)")
    parser.add_argument("--days", type=int, default=7, help="rải dữ liệu lịch sử trong N ngày gần nhất")
    parser.add_argument("--realtime", action="store_true", help="ghi theo thời gian thực")
    parser.add_argument("--delay", type=float, default=0.5, help="độ trễ tối đa giữa 2 sự kiện (giây)")
    parser.add_argument("--concurrent", type=int, default=5, help="số phiên diễn ra đồng thời (realtime)")
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
        mode = "REALTIME" if args.realtime else "BACKFILL"
        print(f"Bắt đầu giả lập ({mode}) với {len(users)} người dùng...")
        if args.realtime:
            run_realtime(writer, users, args.sessions, args.delay, args.concurrent)
        else:
            run_backfill(writer, users, args.sessions, args.days)
    except KeyboardInterrupt:
        print("\nĐã dừng theo yêu cầu.")
    finally:
        if writer:
            elapsed = max(time.time() - started_at, 1e-6)
            print(f"Tổng cộng đã ghi {writer.count} sự kiện trong {elapsed:.1f}s "
                  f"({writer.count / elapsed:.0f} sự kiện/giây).")
        cass.cluster.shutdown()


if __name__ == "__main__":
    main()
