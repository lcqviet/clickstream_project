-- =====================================================================
-- Schema Cassandra: Hệ thống lưu vết hành vi người dùng (Clickstream)
-- Thiết kế theo truy vấn: mỗi truy vấn chính có một bảng riêng.
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS ecommerce_tracking
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
-- Production: dùng NetworkTopologyStrategy với replication_factor = 3

-- ---------------------------------------------------------------------
-- Q1: Lịch sử hành vi của một user theo thời gian (mới nhất trước)
--     SELECT * FROM events_by_user WHERE user_id = 'U0001' AND event_date = '2026-10-11' LIMIT 20;
-- Partition = (user_id, event_date): chặn partition phình vô hạn.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecommerce_tracking.events_by_user (
    user_id         text,
    event_date      date,        -- ngày theo giờ Việt Nam (UTC+7)
    event_time      timestamp,
    event_id        uuid,        -- tránh ghi đè khi trùng mili-giây
    session_id      uuid,
    event_type      text,        -- click | view | add_to_cart | search
    product_id      text,
    category        text,
    search_keyword  text,
    quantity        int,
    page_url        text,
    device          text,
    PRIMARY KEY ((user_id, event_date), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);

-- ---------------------------------------------------------------------
-- Q2: Hành trình clickstream trong một phiên truy cập (theo thứ tự thời gian)
--     SELECT * FROM events_by_session WHERE session_id = <uuid>;
-- Một phiên chỉ có vài chục sự kiện nên partition luôn nhỏ.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecommerce_tracking.events_by_session (
    session_id      uuid,
    event_time      timestamp,
    event_id        uuid,
    user_id         text,
    event_date      date,
    event_type      text,
    product_id      text,
    category        text,
    search_keyword  text,
    quantity        int,
    page_url        text,
    device          text,
    PRIMARY KEY ((session_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time ASC, event_id ASC);

-- ---------------------------------------------------------------------
-- Q3: Thống kê lượt view / click / add_to_cart của sản phẩm theo ngày
--     SELECT product_id, event_type, cnt FROM product_stats_by_day WHERE event_date = '2026-10-11';
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecommerce_tracking.product_stats_by_day (
    event_date      date,
    product_id      text,
    event_type      text,
    cnt             counter,
    PRIMARY KEY ((event_date), product_id, event_type)
);

-- ---------------------------------------------------------------------
-- Q4: Từ khóa tìm kiếm phổ biến theo ngày
--     SELECT search_keyword, cnt FROM keyword_stats_by_day WHERE event_date = '2026-10-11';
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecommerce_tracking.keyword_stats_by_day (
    event_date      date,
    search_keyword  text,
    cnt             counter,
    PRIMARY KEY ((event_date), search_keyword)
);
