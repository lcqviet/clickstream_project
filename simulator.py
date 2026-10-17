"""
simulator.py - Giả lập website TMĐT sinh sự kiện clickstream và ghi vào hệ thống (GUI version).
"""
import random
import time
import uuid
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
import threading
import tkinter as tk
from tkinter import ttk, messagebox
import queue

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
POPULARITY = {pid: 1 / (i + 1) ** 0.7 for i, pid in enumerate(PRODUCT_IDS)}
POOL_BY_CATEGORY = {c: [p for p in PRODUCT_IDS if PRODUCTS[p][1] == c] for c in CATEGORIES}


def utcnow():
    return datetime.now(timezone.utc)


def pick_product(category):
    pool = POOL_BY_CATEGORY[category]
    return random.choices(pool, weights=[POPULARITY[p] for p in pool])[0]


def with_retry(fn, attempts=5):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as err:
            if i == attempts:
                raise
            wait = 2 * i
            print(f"\n  ! lỗi tạm thời ({type(err).__name__}), thử lại sau {wait}s ({i}/{attempts})")
            time.sleep(wait)


# ---------------------------------------------------------------------------
# 1. Sinh hành vi người dùng
# ---------------------------------------------------------------------------
def generate_session(user_id, start):
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

    for _ in range(random.randint(1, 4)):
        if random.random() < 0.5:
            keyword = random.choice(list(KEYWORDS))
            category = KEYWORDS[keyword]
            list_url = "/search?q=" + keyword.replace(" ", "+")
            emit("search", list_url, keyword=keyword)
        else:
            category = random.choice(CATEGORIES)
            list_url = f"/category/{category}"

        for _ in range(random.randint(1, 3)):
            pid = pick_product(category)
            emit("click", list_url, product_id=pid)
            emit("view", f"/product/{pid}", product_id=pid)
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
            "UPDATE product_stats_by_day SET cnt = cnt + ? "
            "WHERE event_date = ? AND product_id = ? AND event_type = ?")
        self.cnt_keyword = cass.prepare(
            "UPDATE keyword_stats_by_day SET cnt = cnt + ? "
            "WHERE event_date = ? AND search_keyword = ?")

    @staticmethod
    def _params(ev):
        def v(x):
            return UNSET_VALUE if x is None else x

        return (
            ev["user_id"],
            to_vn(ev["event_time"]).date(),
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

        batch = BatchStatement(batch_type=BatchType.LOGGED)
        batch.add(self.ins_user, params)
        batch.add(self.ins_session, params)
        self.cass.execute(batch)

        if ev["product_id"]:
            self.cass.execute(self.cnt_product, (1, day, ev["product_id"], ev["event_type"]))
        if ev["event_type"] == "search":
            self.cass.execute(self.cnt_keyword, (1, day, ev["search_keyword"]))

        if ev["event_type"] == "view":
            score = int(ev["event_time"].timestamp() * 1000)
            self._update_recent({ev["user_id"]: {ev["product_id"]: score}})

        self.count += 1

    def write_chunk(self, events, concurrency=64):
        statements = []
        product_cnt = Counter()
        keyword_cnt = Counter()
        recent = defaultdict(dict)

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

        with_retry(lambda: execute_concurrent(
            self.cass, statements, concurrency=concurrency, raise_on_first_error=True))

        for (day, pid, etype), n in product_cnt.items():
            with_retry(lambda: self.cass.execute(self.cnt_product, (n, day, pid, etype)))
        for (day, kw), n in keyword_cnt.items():
            with_retry(lambda: self.cass.execute(self.cnt_keyword, (n, day, kw)))

        self._update_recent(recent)
        self.count += len(events)

    def _update_recent(self, recent_by_user):
        pipe = self.rds.pipeline(transaction=False)
        for user_id, views in recent_by_user.items():
            key = recent_views_key(user_id)
            top = dict(sorted(views.items(), key=lambda kv: kv[1], reverse=True)[:RECENT_VIEW_LIMIT])
            pipe.zadd(key, top, gt=True)
            pipe.zremrangebyrank(key, 0, -(RECENT_VIEW_LIMIT + 1))
            pipe.expire(key, RECENT_VIEW_TTL)
        pipe.execute()

# ---------------------------------------------------------------------------
# Các hàm tiện ích cho GUI
# ---------------------------------------------------------------------------
def reset_all_data(cass, rds):
    for table in CASSANDRA_TABLES:
        cass.execute(f"TRUNCATE {table}")
    for key in rds.scan_iter("user:*:recent_views"):
        rds.delete(key)

# ---------------------------------------------------------------------------
# Lớp giao diện chính
# ---------------------------------------------------------------------------
class SimulatorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Giả Lập Dữ Liệu Clickstream TMĐT")
        self.root.geometry("600x700")

        # Cấu hình grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # Main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.main_frame.columnconfigure(1, weight=1)

        # Variables
        self.var_users = tk.IntVar(value=1000)
        self.var_reset = tk.BooleanVar(value=False)
        self.var_seed = tk.StringVar(value="")
        
        self.var_mode = tk.StringVar(value="bulk") # "bulk" or "realtime"

        # Bulk vars
        self.var_events = tk.IntVar(value=1000000)
        self.var_days = tk.IntVar(value=7)
        self.var_chunk = tk.IntVar(value=5000)
        self.var_concurrency = tk.IntVar(value=64)

        # Realtime vars
        self.var_sessions = tk.IntVar(value=0)
        self.var_delay = tk.DoubleVar(value=0.5)
        self.var_active_sessions = tk.IntVar(value=5)

        # Threading support
        self.is_running = False
        self.worker_thread = None
        self.stop_event = threading.Event()
        self.msg_queue = queue.Queue()

        self.create_widgets()
        self.root.after(100, self.process_queue)

    def create_widgets(self):
        row = 0

        # General Settings
        ttk.Label(self.main_frame, text="Cài đặt chung", font=("Helvetica", 12, "bold")).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=(0, 10))
        row += 1

        ttk.Label(self.main_frame, text="Số người dùng (users):").grid(row=row, column=0, sticky=tk.W)
        ttk.Entry(self.main_frame, textvariable=self.var_users).grid(row=row, column=1, sticky=(tk.W, tk.E))
        row += 1

        ttk.Label(self.main_frame, text="Seed (tùy chọn):").grid(row=row, column=0, sticky=tk.W)
        ttk.Entry(self.main_frame, textvariable=self.var_seed).grid(row=row, column=1, sticky=(tk.W, tk.E))
        row += 1

        ttk.Checkbutton(self.main_frame, text="Xóa sạch dữ liệu cũ trước khi chạy (Reset)", variable=self.var_reset).grid(row=row, column=0, columnspan=2, sticky=tk.W)
        row += 1
        
        ttk.Separator(self.main_frame, orient=tk.HORIZONTAL).grid(row=row, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)
        row+=1

        # Mode Selection
        ttk.Label(self.main_frame, text="Chế độ chạy", font=("Helvetica", 12, "bold")).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=(0, 10))
        row += 1

        mode_frame = ttk.Frame(self.main_frame)
        mode_frame.grid(row=row, column=0, columnspan=2, sticky=tk.W)
        ttk.Radiobutton(mode_frame, text="Chế độ BULK (Ghi hàng loạt)", variable=self.var_mode, value="bulk", command=self.update_ui_state).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(mode_frame, text="Chế độ REALTIME", variable=self.var_mode, value="realtime", command=self.update_ui_state).pack(side=tk.LEFT, padx=5)
        row += 1

        # Bulk Settings
        self.bulk_frame = ttk.LabelFrame(self.main_frame, text="Cài đặt BULK", padding="10")
        self.bulk_frame.grid(row=row, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        self.bulk_frame.columnconfigure(1, weight=1)
        row += 1

        ttk.Label(self.bulk_frame, text="Tổng sự kiện (events):").grid(row=0, column=0, sticky=tk.W)
        self.entry_events = ttk.Entry(self.bulk_frame, textvariable=self.var_events)
        self.entry_events.grid(row=0, column=1, sticky=(tk.W, tk.E))

        ttk.Label(self.bulk_frame, text="Số ngày (days):").grid(row=1, column=0, sticky=tk.W)
        self.entry_days = ttk.Entry(self.bulk_frame, textvariable=self.var_days)
        self.entry_days.grid(row=1, column=1, sticky=(tk.W, tk.E))

        ttk.Label(self.bulk_frame, text="Sự kiện mỗi lô (chunk):").grid(row=2, column=0, sticky=tk.W)
        self.entry_chunk = ttk.Entry(self.bulk_frame, textvariable=self.var_chunk)
        self.entry_chunk.grid(row=2, column=1, sticky=(tk.W, tk.E))

        ttk.Label(self.bulk_frame, text="Số luồng ghi (concurrency):").grid(row=3, column=0, sticky=tk.W)
        self.entry_concurrency = ttk.Entry(self.bulk_frame, textvariable=self.var_concurrency)
        self.entry_concurrency.grid(row=3, column=1, sticky=(tk.W, tk.E))

        # Realtime Settings
        self.realtime_frame = ttk.LabelFrame(self.main_frame, text="Cài đặt REALTIME", padding="10")
        self.realtime_frame.grid(row=row, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)
        self.realtime_frame.columnconfigure(1, weight=1)
        row += 1

        ttk.Label(self.realtime_frame, text="Số phiên (sessions, 0=chạy mãi):").grid(row=0, column=0, sticky=tk.W)
        self.entry_sessions = ttk.Entry(self.realtime_frame, textvariable=self.var_sessions)
        self.entry_sessions.grid(row=0, column=1, sticky=(tk.W, tk.E))

        ttk.Label(self.realtime_frame, text="Độ trễ tối đa (delay):").grid(row=1, column=0, sticky=tk.W)
        self.entry_delay = ttk.Entry(self.realtime_frame, textvariable=self.var_delay)
        self.entry_delay.grid(row=1, column=1, sticky=(tk.W, tk.E))

        ttk.Label(self.realtime_frame, text="Phiên đồng thời (active_sessions):").grid(row=2, column=0, sticky=tk.W)
        self.entry_active = ttk.Entry(self.realtime_frame, textvariable=self.var_active_sessions)
        self.entry_active.grid(row=2, column=1, sticky=(tk.W, tk.E))

        # Control Buttons
        btn_frame = ttk.Frame(self.main_frame)
        btn_frame.grid(row=row, column=0, columnspan=2, pady=15)
        row += 1

        self.btn_start = ttk.Button(btn_frame, text="Bắt đầu", command=self.start_simulation)
        self.btn_start.pack(side=tk.LEFT, padx=5)

        self.btn_stop = ttk.Button(btn_frame, text="Dừng", command=self.stop_simulation, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        # Log Text Area
        ttk.Label(self.main_frame, text="Nhật ký chạy:").grid(row=row, column=0, sticky=tk.W)
        row += 1
        
        self.text_log = tk.Text(self.main_frame, height=10, width=60, state=tk.DISABLED)
        self.text_log.grid(row=row, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Scrollbar cho Text
        scrollbar = ttk.Scrollbar(self.main_frame, orient=tk.VERTICAL, command=self.text_log.yview)
        scrollbar.grid(row=row, column=2, sticky=(tk.N, tk.S))
        self.text_log['yscrollcommand'] = scrollbar.set

        self.main_frame.rowconfigure(row, weight=1) # Cho phép log area mở rộng
        
        self.update_ui_state()

    def update_ui_state(self):
        mode = self.var_mode.get()
        if mode == "bulk":
            self.enable_children(self.bulk_frame)
            self.disable_children(self.realtime_frame)
        else:
            self.disable_children(self.bulk_frame)
            self.enable_children(self.realtime_frame)

    def enable_children(self, parent):
        for child in parent.winfo_children():
            child.configure(state=tk.NORMAL)

    def disable_children(self, parent):
        for child in parent.winfo_children():
            child.configure(state=tk.DISABLED)

    def log_message(self, message):
        self.msg_queue.put(("log", message))

    def process_queue(self):
        try:
            while True:
                msg_type, msg = self.msg_queue.get_nowait()
                if msg_type == "log":
                    self.text_log.configure(state=tk.NORMAL)
                    self.text_log.insert(tk.END, msg + "\n")
                    self.text_log.see(tk.END)
                    self.text_log.configure(state=tk.DISABLED)
                elif msg_type == "done":
                    self.is_running = False
                    self.btn_start.configure(state=tk.NORMAL)
                    self.btn_stop.configure(state=tk.DISABLED)
                    messagebox.showinfo("Thông báo", "Giả lập hoàn tất!")
        except queue.Empty:
            pass
        self.root.after(100, self.process_queue)

    def start_simulation(self):
        if self.is_running:
            return

        # Đọc tham số
        try:
            users_count = self.var_users.get()
            if users_count <= 0: raise ValueError("Số người dùng phải lớn hơn 0")
            
            seed_val = self.var_seed.get()
            if seed_val:
                random.seed(int(seed_val))

            mode = self.var_mode.get()
        except ValueError as e:
            messagebox.showerror("Lỗi đầu vào", str(e))
            return

        self.is_running = True
        self.stop_event.clear()
        self.btn_start.configure(state=tk.DISABLED)
        self.btn_stop.configure(state=tk.NORMAL)
        
        self.text_log.configure(state=tk.NORMAL)
        self.text_log.delete(1.0, tk.END)
        self.text_log.configure(state=tk.DISABLED)

        self.worker_thread = threading.Thread(target=self.run_simulation_task, args=(users_count, mode))
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def stop_simulation(self):
        if self.is_running:
            self.log_message("Đang yêu cầu dừng giả lập...")
            self.stop_event.set()

    def run_simulation_task(self, users_count, mode):
        cass = None
        writer = None
        started_at = time.time()
        try:
            self.log_message("Đang kết nối CSDL...")
            cass = connect_cassandra(KEYSPACE)
            rds = connect_redis()
            users = [f"U{i:04d}" for i in range(1, users_count + 1)]

            if self.var_reset.get():
                self.log_message("Đang xóa dữ liệu cũ...")
                reset_all_data(cass, rds)
                self.log_message("Đã xóa dữ liệu cũ.")

            writer = Writer(cass, rds)

            if mode == "bulk":
                events = self.var_events.get()
                days = self.var_days.get()
                chunk = self.var_chunk.get()
                concurrency = self.var_concurrency.get()
                
                self.log_message(f"Bắt đầu chạy BULK: {events:,} sự kiện, {len(users)} users, trong {days} ngày...")
                self._run_bulk_with_stop(writer, users, events, days, chunk, concurrency)
            else:
                sessions = self.var_sessions.get()
                delay = self.var_delay.get()
                active = self.var_active_sessions.get()
                
                self.log_message(f"Bắt đầu chạy REALTIME: {len(users)} users...")
                self._run_realtime_with_stop(writer, users, sessions, delay, active)

        except Exception as e:
            self.log_message(f"Lỗi: {e}")
        finally:
            if writer:
                elapsed = max(time.time() - started_at, 1e-6)
                msg = f"Đã ghi {writer.count:,} sự kiện trong {elapsed:.1f}s ({writer.count / elapsed:,.0f} sự kiện/giây)."
                self.log_message(msg)
            if cass:
                cass.cluster.shutdown()
            self.msg_queue.put(("done", ""))

    def _run_bulk_with_stop(self, writer, users, total, days, chunk, concurrency):
        now = utcnow()
        buffer, produced = [], 0
        started = time.time()

        while produced < total and not self.stop_event.is_set():
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
                
                status_msg = f"{writer.count:>10,}/{total:,} sự kiện ({writer.count / total:6.1%}) | {rate:,.0f} sự kiện/s | còn khoảng {eta / 60:4.1f} phút"
                # Update log without filling it up too fast for bulk
                if produced % (chunk * 5) < chunk:
                    self.log_message(status_msg)

    def _run_realtime_with_stop(self, writer, users, n_sessions, delay, active_sessions):
        limit = n_sessions if n_sessions > 0 else float("inf")
        active, started = [], 0
        while (started < limit or active) and not self.stop_event.is_set():
            while started < limit and len(active) < active_sessions:
                active.append(deque(generate_session(random.choice(users), utcnow())))
                started += 1
            if not active:
                break
                
            queue = random.choice(active)
            ev = queue.popleft()
            ev["event_time"] = utcnow()
            writer.write(ev)
            
            # Format log message
            when = to_vn(ev["event_time"]).strftime("%H:%M:%S")
            detail = ""
            if ev["product_id"]:
                detail = f"{ev['product_id']} {PRODUCTS[ev['product_id']][0]}"
            elif ev["search_keyword"]:
                detail = f"\"{ev['search_keyword']}\""
            log_str = f"[{when}] {ev['user_id']}  {ev['event_type']:<12} {detail}"
            self.log_message(log_str)
            
            if not queue:
                active.remove(queue)
                
            if delay > 0:
                # Ngủ ngắt quãng để phản hồi stop_event nhanh hơn
                sleep_time = random.uniform(0, delay)
                end_sleep = time.time() + sleep_time
                while time.time() < end_sleep and not self.stop_event.is_set():
                    time.sleep(0.05)


def main():
    root = tk.Tk()
    app = SimulatorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()