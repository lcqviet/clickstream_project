#!/usr/bin/env python3
"""query_cli.py - Công cụ dòng lệnh truy vấn dữ liệu clickstream (Cassandra + Redis)."""
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

from common import (
    KEYSPACE,
    RECENT_VIEW_LIMIT,
    VN_TZ,
    connect_cassandra,
    connect_redis,
    product_name,
    recent_views_key,
    to_vn,
    today_vn,
)

EVENT_TYPES = {"click", "view", "add_to_cart", "search"}


# ---------------------------------------------------------------------------
# Lớp truy vấn: mỗi hàm ứng với một câu truy vấn đã được thiết kế trong schema
# ---------------------------------------------------------------------------
class ClickstreamQuery:
    def __init__(self):
        self.cass = connect_cassandra(KEYSPACE)
        self.rds = connect_redis()

        self.q_user_day = self.cass.prepare(
            "SELECT event_time, event_type, product_id, search_keyword, quantity, "
            "page_url, device, session_id FROM events_by_user "
            "WHERE user_id = ? AND event_date = ? LIMIT ?")
        self.q_session = self.cass.prepare(
            "SELECT event_time, event_type, product_id, search_keyword, quantity, "
            "page_url, device, user_id FROM events_by_session WHERE session_id = ?")
        self.q_product_day = self.cass.prepare(
            "SELECT product_id, event_type, cnt FROM product_stats_by_day WHERE event_date = ?")
        self.q_keyword_day = self.cass.prepare(
            "SELECT search_keyword, cnt FROM keyword_stats_by_day WHERE event_date = ?")

    # Q0 - Redis: 5 sản phẩm xem gần nhất (mới nhất trước)
    def recent_views(self, user_id):
        key = recent_views_key(user_id)
        items = self.rds.zrevrange(key, 0, RECENT_VIEW_LIMIT - 1, withscores=True)
        return items, self.rds.ttl(key)

    # Q1 - Cassandra: lịch sử hành vi của user trong N ngày gần nhất
    def user_history(self, user_id, days=1, event_type=None, limit=30):
        rows, today = [], today_vn()
        for offset in range(days):  # mỗi ngày là một partition riêng
            day = today - timedelta(days=offset)
            fetch = limit if not event_type else 1000
            for row in self.cass.execute(self.q_user_day, (user_id, day, fetch)):
                if event_type and row.event_type != event_type:
                    continue
                rows.append(row)
                if len(rows) >= limit:
                    return rows
        return rows

    # Q2 - Cassandra: toàn bộ hành trình của một phiên
    def session_path(self, session_id):
        return list(self.cass.execute(self.q_session, (session_id,)))

    # Q3 - Cassandra: thống kê sản phẩm theo ngày
    def product_stats(self, day):
        stats = defaultdict(lambda: defaultdict(int))
        for r in self.cass.execute(self.q_product_day, (day,)):
            stats[r.product_id][r.event_type] = r.cnt
        return stats

    # Q4 - Cassandra: từ khóa tìm kiếm theo ngày
    def keyword_stats(self, day):
        rows = [(r.search_keyword, r.cnt) for r in self.cass.execute(self.q_keyword_day, (day,))]
        return sorted(rows, key=lambda x: x[1], reverse=True)

    def close(self):
        self.cass.cluster.shutdown()
        self.rds.close()


# ---------------------------------------------------------------------------
# Giao diện dòng lệnh
# ---------------------------------------------------------------------------
def ask(prompt, default=""):
    suffix = f" [{default}]" if default else ""
    return input(f"{prompt}{suffix}: ").strip() or default


def parse_day(text):
    return datetime.strptime(text, "%Y-%m-%d").date() if text else today_vn()


def fmt_time(dt):
    return to_vn(dt).strftime("%Y-%m-%d %H:%M:%S")


def describe(row):
    if row.product_id:
        text = f"{row.product_id} {product_name(row.product_id)}"
        return text + (f" x{row.quantity}" if row.quantity else "")
    if row.search_keyword:
        return f'"{row.search_keyword}"'
    return ""


def menu_recent(svc):
    user_id = ask("User ID", "U0001")
    items, ttl = svc.recent_views(user_id)
    print(f"\n[Redis] {RECENT_VIEW_LIMIT} sản phẩm xem gần nhất của {user_id}  "
          f"(key: {recent_views_key(user_id)})")
    if not items:
        print("  (chưa có dữ liệu)")
        return
    for rank, (pid, score) in enumerate(items, 1):
        when = datetime.fromtimestamp(score / 1000, tz=VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {rank}. {pid}  {product_name(pid):<32} {when}")
    print(f"  TTL còn lại: {ttl} giây")


def menu_history(svc):
    user_id = ask("User ID", "U0001")
    days = int(ask("Số ngày gần nhất", "1"))
    etype = ask("Lọc loại sự kiện (click/view/add_to_cart/search, Enter = tất cả)")
    if etype and etype not in EVENT_TYPES:
        raise ValueError(f"loại sự kiện phải thuộc {sorted(EVENT_TYPES)}")
    limit = int(ask("Số dòng tối đa", "30"))

    rows = svc.user_history(user_id, days, etype or None, limit)
    print(f"\n[Cassandra] Lịch sử hành vi của {user_id} ({len(rows)} sự kiện, mới nhất trước)")
    if not rows:
        print("  (không có dữ liệu)")
        return
    print(f"  {'Thời gian':<20}{'Sự kiện':<13}{'Chi tiết':<40}{'Thiết bị':<9}Session")
    for r in rows:
        print(f"  {fmt_time(r.event_time):<20}{r.event_type:<13}{describe(r)[:38]:<40}"
              f"{r.device:<9}{r.session_id}")


def menu_session(svc):
    session_id = uuid.UUID(ask("Session ID (copy từ chức năng 2)"))
    rows = svc.session_path(session_id)
    if not rows:
        print("Không tìm thấy phiên này.")
        return
    duration = int((rows[-1].event_time - rows[0].event_time).total_seconds())
    print(f"\n[Cassandra] Phiên {session_id}\n  user {rows[0].user_id} | thiết bị {rows[0].device} "
          f"| {len(rows)} sự kiện | kéo dài {duration}s")
    for i, r in enumerate(rows, 1):
        print(f"  {i:>2}. {fmt_time(r.event_time)}  {r.event_type:<12} {describe(r):<42} {r.page_url}")


def menu_products(svc):
    day = parse_day(ask("Ngày (YYYY-MM-DD, Enter = hôm nay)"))
    top = int(ask("Hiển thị top", "10"))
    stats = svc.product_stats(day)
    if not stats:
        print("Không có dữ liệu cho ngày này.")
        return
    ranked = sorted(stats.items(), key=lambda kv: kv[1]["view"], reverse=True)[:top]
    print(f"\n[Cassandra] Thống kê sản phẩm ngày {day}")
    print(f"  {'Hạng':<6}{'Mã':<6}{'Sản phẩm':<32}{'View':>6}{'Click':>7}{'Giỏ hàng':>10}{'Giỏ/View':>10}")
    for rank, (pid, c) in enumerate(ranked, 1):
        rate = f"{c['add_to_cart'] / c['view'] * 100:.1f}%" if c["view"] else "-"
        print(f"  {rank:<6}{pid:<6}{product_name(pid):<32}{c['view']:>6}{c['click']:>7}"
              f"{c['add_to_cart']:>10}{rate:>10}")


def menu_keywords(svc):
    day = parse_day(ask("Ngày (YYYY-MM-DD, Enter = hôm nay)"))
    top = int(ask("Hiển thị top", "10"))
    rows = svc.keyword_stats(day)[:top]
    if not rows:
        print("Không có dữ liệu cho ngày này.")
        return
    print(f"\n[Cassandra] Từ khóa tìm kiếm phổ biến ngày {day}")
    for rank, (kw, cnt) in enumerate(rows, 1):
        print(f"  {rank:>2}. {kw:<25}{cnt:>6} lượt")


MENU = """
================ CLICKSTREAM QUERY TOOL ================
 1. 5 sản phẩm khách hàng vừa xem gần nhất        (Redis)
 2. Lịch sử hành vi của một khách hàng            (Cassandra)
 3. Hành trình clickstream của một phiên          (Cassandra)
 4. Thống kê tương tác sản phẩm theo ngày         (Cassandra)
 5. Từ khóa tìm kiếm phổ biến theo ngày           (Cassandra)
 0. Thoát
========================================================"""


def main():
    svc = ClickstreamQuery()
    actions = {"1": menu_recent, "2": menu_history, "3": menu_session,
               "4": menu_products, "5": menu_keywords}
    try:
        while True:
            print(MENU)
            choice = input("Chọn chức năng: ").strip()
            if choice == "0":
                break
            handler = actions.get(choice)
            if handler is None:
                print("Lựa chọn không hợp lệ.")
                continue
            try:
                handler(svc)
            except ValueError as err:
                print(f"Dữ liệu nhập không hợp lệ: {err}")
            except Exception as err:  # noqa: BLE001 - hiển thị lỗi thân thiện cho người dùng
                print(f"Lỗi khi truy vấn: {err}")
    except (KeyboardInterrupt, EOFError):
        print()
    finally:
        svc.close()


if __name__ == "__main__":
    main()
