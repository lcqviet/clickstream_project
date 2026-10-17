"""
query_gui.py - Giao diện đồ họa (Tkinter) để truy vấn dữ liệu clickstream trong Cassandra và Redis.

Chạy:  python query_gui.py
Dùng lại lớp ClickstreamQuery của query_cli.py nên các truy vấn CQL / lệnh Redis giống hệt bản dòng lệnh.
Mọi truy vấn chạy ở luồng nền để cửa sổ không bị treo.
"""
import functools
import queue
import threading
import time
import tkinter as tk
from collections import defaultdict
from datetime import datetime
from tkinter import messagebox, ttk
import uuid

from common import (
    CASSANDRA_HOSTS,
    CASSANDRA_PORT,
    PRODUCTS,
    REDIS_HOST,
    REDIS_PORT,
    RECENT_VIEW_LIMIT,
    VN_TZ,
    product_name,
    recent_views_key,
    today_vn,
)
from query_cli import EVENT_TYPES, ClickstreamQuery, describe, fmt_time, parse_day

FONT = ("Segoe UI", 10)
FONT_BOLD = ("Segoe UI", 10, "bold")
TEAL = "#0F6E56"


# ---------------------------------------------------------------------------
# Biểu đồ cột ngang vẽ bằng Canvas (không cần thư viện ngoài)
# ---------------------------------------------------------------------------
class BarChart(tk.Canvas):
    ROW = 24

    def __init__(self, master, color="#1D9E75"):
        super().__init__(master, background="white", highlightthickness=1,
                         highlightbackground="#D0D7DE", height=60)
        self.color, self.items, self.title = color, [], ""
        self.bind("<Configure>", lambda _e: self._draw())

    def set_data(self, items, title=""):
        self.items, self.title = items, title
        self.configure(height=40 + self.ROW * max(len(items), 1))
        self._draw()

    def _draw(self):
        self.delete("all")
        width = self.winfo_width()
        if width < 120:
            return
        self.create_text(10, 16, text=self.title, anchor="w", font=FONT_BOLD, fill="#24292F")
        if not self.items:
            self.create_text(10, 48, text="(không có dữ liệu)", anchor="w", font=FONT, fill="#888888")
            return
        label_w = 240
        top = max(v for _, v in self.items) or 1
        bar_max = max(width - label_w - 100, 60)
        for i, (label, value) in enumerate(self.items):
            y = 36 + i * self.ROW
            text = label if len(label) <= 34 else label[:33] + "…"
            self.create_text(label_w - 8, y + 11, text=text, anchor="e", font=FONT, fill="#24292F")
            bar = max(2, int(bar_max * value / top))
            self.create_rectangle(label_w, y + 3, label_w + bar, y + 19, fill=self.color, outline="")
            self.create_text(label_w + bar + 6, y + 11, text=f"{value:,}", anchor="w", font=FONT, fill="#24292F")


def guarded(fn):
    """Bắt lỗi nhập liệu (ValueError) và báo bằng hộp thoại thay vì làm sập chương trình."""
    @functools.wraps(fn)
    def wrapper(self, *args):
        try:
            return fn(self, *args)
        except ValueError as err:
            messagebox.showwarning("Dữ liệu nhập chưa hợp lệ", str(err), parent=self)
    return wrapper


# ---------------------------------------------------------------------------
# Ứng dụng chính
# ---------------------------------------------------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Hệ thống lưu vết hành vi người dùng - Truy vấn Clickstream")
        self.geometry("1180x740")
        self.minsize(980, 620)
        self.svc = None
        self._jobs = queue.Queue()

        self._setup_style()
        self._build_header()
        self._build_status()  # tạo trước để luôn nằm dưới cùng
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=8, pady=(8, 0))
        for build in (self._tab_recent, self._tab_history, self._tab_session, self._tab_products,
                      self._tab_keywords, self._tab_overview, self._tab_count,
                      self._tab_trend, self._tab_sessions):
            build()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._poll)
        self._connect()

    # ----- giao diện chung -----
    def _setup_style(self):
        style = ttk.Style(self)
        for theme in ("vista", "clam"):
            if theme in style.theme_names():
                style.theme_use(theme)
                break
        style.configure(".", font=FONT)
        style.configure("Treeview", rowheight=26, font=FONT)
        style.configure("Treeview.Heading", font=FONT_BOLD)
        style.configure("TNotebook.Tab", padding=(12, 6), font=FONT)
        style.configure("Big.TLabel", font=("Segoe UI", 22, "bold"), foreground=TEAL)
        style.configure("Hint.TLabel", foreground="#57606A")

    def _build_header(self):
        bar = tk.Frame(self, background=TEAL)
        bar.pack(fill="x")
        tk.Label(bar, text="HỆ THỐNG LƯU VẾT HÀNH VI NGƯỜI DÙNG TRÊN WEBSITE TMĐT", bg=TEAL, fg="white",
                 font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=14, pady=(10, 0))
        tk.Label(bar, text="Truy vấn dữ liệu Clickstream  |  Cassandra (log đầy đủ)  +  Redis (5 sản phẩm vừa xem)",
                 bg=TEAL, fg="#CFEDE2", font=FONT).pack(anchor="w", padx=14, pady=(0, 10))

    def _build_status(self):
        bar = ttk.Frame(self, padding=(10, 4))
        bar.pack(side="bottom", fill="x")
        self.status = ttk.Label(bar, text="")
        self.status.pack(side="left")
        self.reconnect_btn = ttk.Button(bar, text="Kết nối lại", command=self._connect)
        self.progress = ttk.Progressbar(bar, mode="indeterminate", length=140)
        self.progress.pack(side="right")

    def set_status(self, text, busy=False, error=False):
        self.status.configure(text=text, foreground="#B42318" if error else "#24292F")
        if busy:
            self.progress.start(12)
        else:
            self.progress.stop()

    # ----- chạy truy vấn ở luồng nền -----
    def run_async(self, work, done, on_error=None):
        if self.svc is None:
            messagebox.showwarning("Chưa kết nối", "Chương trình chưa kết nối được tới cơ sở dữ liệu.", parent=self)
            return
        self.set_status("Đang truy vấn...", busy=True)

        def target():
            start = time.time()
            try:
                result = work()
                self._jobs.put((done, on_error, result, None, time.time() - start))
            except BaseException as err:  # noqa: BLE001 - kể cả SystemExit từ hàm kết nối
                self._jobs.put((done, on_error, None, err, time.time() - start))

        threading.Thread(target=target, daemon=True).start()

    def _poll(self):
        try:
            while True:
                done, on_error, result, err, elapsed = self._jobs.get_nowait()
                if err is not None:
                    self.set_status("Truy vấn thất bại.", error=True)
                    (on_error or self._default_error)(err)
                else:
                    done(result)
                    if elapsed and self.svc is not None:
                        self.set_status(f"Hoàn tất trong {elapsed:.2f} giây.   |   Cassandra "
                                        f"{CASSANDRA_HOSTS[0]}:{CASSANDRA_PORT}   |   Redis {REDIS_HOST}:{REDIS_PORT}")
        except queue.Empty:
            pass
        self.after(100, self._poll)

    def _default_error(self, err):
        messagebox.showerror("Lỗi truy vấn", str(err) or type(err).__name__, parent=self)

    # ----- kết nối -----
    def _connect(self):
        self.reconnect_btn.pack_forget()
        self.set_status("Đang kết nối Cassandra và Redis (tối đa khoảng 60 giây nếu Docker vừa khởi động)...", busy=True)

        def target():
            try:
                self._jobs.put((self._on_connected, self._on_connect_failed, ClickstreamQuery(), None, 0))
            except BaseException as err:  # noqa: BLE001
                self._jobs.put((self._on_connected, self._on_connect_failed, None, err, 0))

        threading.Thread(target=target, daemon=True).start()

    def _on_connected(self, svc):
        self.svc = svc
        self.set_status(f"Đã kết nối: Cassandra {CASSANDRA_HOSTS[0]}:{CASSANDRA_PORT}   |   Redis {REDIS_HOST}:{REDIS_PORT}")

    def _on_connect_failed(self, err):
        self.set_status("Không kết nối được cơ sở dữ liệu.", error=True)
        self.reconnect_btn.pack(side="left", padx=10)
        messagebox.showerror("Không kết nối được",
                             f"{err}\n\nHãy kiểm tra Docker đã chạy (docker compose ps) rồi bấm 'Kết nối lại'.",
                             parent=self)

    def _on_close(self):
        try:
            if self.svc is not None:
                self.svc.close()
        finally:
            self.destroy()

    # ----- tiện ích dựng form -----
    def _tab(self, title):
        frame = ttk.Frame(self.nb, padding=12)
        self.nb.add(frame, text=title)
        return frame

    @staticmethod
    def _row(parent):
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=(0, 8))
        return row

    @staticmethod
    def _field(row, label, var, width=14, kind="entry", values=None, lo=1, hi=999999):
        ttk.Label(row, text=label).pack(side="left", padx=(0, 6))
        if kind == "combo":
            w = ttk.Combobox(row, textvariable=var, values=values, width=width, state="readonly")
        elif kind == "spin":
            w = ttk.Spinbox(row, textvariable=var, from_=lo, to=hi, width=width)
        else:
            w = ttk.Entry(row, textvariable=var, width=width)
        w.pack(side="left", padx=(0, 16))
        return w

    def _tree(self, parent, columns, expand=True, height=10):
        """columns: [(id, tiêu đề, độ rộng, canh lề)]"""
        box = ttk.Frame(parent)
        box.pack(fill="both" if expand else "x", expand=expand)
        tree = ttk.Treeview(box, columns=[c[0] for c in columns], show="headings", height=height)
        for cid, text, width, anchor in columns:
            tree.heading(cid, text=text, command=lambda c=cid, t=tree: self._sort(t, c))
            tree.column(cid, width=width, anchor=anchor, stretch=(cid == columns[-1][0] or width > 200))
        ysb = ttk.Scrollbar(box, orient="vertical", command=tree.yview)
        xsb = ttk.Scrollbar(box, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        ysb.grid(row=0, column=1, sticky="ns")
        xsb.grid(row=1, column=0, sticky="ew")
        box.rowconfigure(0, weight=1)
        box.columnconfigure(0, weight=1)
        tree.tag_configure("odd", background="#F3F7F6")
        tree.tag_configure("total", font=FONT_BOLD, background="#DCEFE8")
        return tree

    @staticmethod
    def _fill(tree, rows, total_last=False):
        tree.delete(*tree.get_children())
        for i, row in enumerate(rows):
            tag = "total" if total_last and i == len(rows) - 1 else ("odd" if i % 2 else "")
            tree.insert("", "end", values=row, tags=(tag,))

    @staticmethod
    def _sort(tree, col):
        """Nhấp vào tiêu đề cột để sắp xếp (tăng/giảm dần)."""
        def key(v):
            try:
                return (0, float(v.replace(",", "").replace("%", "")))
            except ValueError:
                return (1, v)
        items = [(tree.set(k, col), k) for k in tree.get_children("") if "total" not in tree.item(k, "tags")]
        reverse = getattr(tree, "_sort_desc", {}).get(col, True)
        items.sort(key=lambda x: key(x[0]), reverse=reverse)
        tree._sort_desc = {col: not reverse}
        for index, (_, k) in enumerate(items):
            tree.move(k, "", index)

    # ----- đọc dữ liệu nhập -----
    @staticmethod
    def _read_int(var, name, lo, hi):
        try:
            value = int(str(var.get()).strip())
        except ValueError:
            raise ValueError(f"{name} phải là số nguyên từ {lo} đến {hi}.") from None
        if not lo <= value <= hi:
            raise ValueError(f"{name} phải nằm trong khoảng {lo} đến {hi}.")
        return value

    @staticmethod
    def _read_day(var):
        try:
            return parse_day(var.get().strip())
        except ValueError:
            raise ValueError("Ngày phải có dạng YYYY-MM-DD, ví dụ 2026-10-11.") from None

    # =====================================================================
    # Chức năng 1: 5 sản phẩm vừa xem (Redis)
    # =====================================================================
    def _tab_recent(self):
        tab = self._tab("1. Sản phẩm vừa xem (Redis)")
        self.v_recent_user = tk.StringVar(value="U0001")
        row = self._row(tab)
        entry = self._field(row, "Mã khách hàng:", self.v_recent_user)
        entry.bind("<Return>", lambda _e: self.do_recent())
        ttk.Button(row, text="Truy vấn", command=self.do_recent).pack(side="left")
        self.l_recent = ttk.Label(tab, text="Nguồn: Redis Sorted Set  user:{id}:recent_views  (ZREVRANGE 0 4)", style="Hint.TLabel")
        self.l_recent.pack(anchor="w", pady=(0, 8))
        self.t_recent = self._tree(tab, [("rank", "Hạng", 60, "center"), ("pid", "Mã SP", 90, "center"),
                                         ("name", "Tên sản phẩm", 380, "w"), ("when", "Thời điểm xem gần nhất", 200, "center")])

    @guarded
    def do_recent(self):
        user = self.v_recent_user.get().strip()
        if not user:
            raise ValueError("Vui lòng nhập mã khách hàng, ví dụ U0001.")

        def done(res):
            items, ttl = res
            rows = [(i, pid, product_name(pid),
                     datetime.fromtimestamp(score / 1000, tz=VN_TZ).strftime("%Y-%m-%d %H:%M:%S"))
                    for i, (pid, score) in enumerate(items, 1)]
            self._fill(self.t_recent, rows)
            self.l_recent.configure(
                text=(f"Khóa Redis: {recent_views_key(user)}   |   Tối đa {RECENT_VIEW_LIMIT} sản phẩm   |   TTL còn lại: {ttl} giây"
                      if items else f"Không có dữ liệu cho khách hàng {user}."))

        self.run_async(lambda: self.svc.recent_views(user), done)

    # =====================================================================
    # Chức năng 2: lịch sử hành vi (Cassandra)
    # =====================================================================
    def _tab_history(self):
        tab = self._tab("2. Lịch sử hành vi")
        self.v_h_user = tk.StringVar(value="U0001")
        self.v_h_days = tk.StringVar(value="1")
        self.v_h_type = tk.StringVar(value="Tất cả")
        self.v_h_limit = tk.StringVar(value="50")
        row = self._row(tab)
        entry = self._field(row, "Mã khách hàng:", self.v_h_user, 12)
        entry.bind("<Return>", lambda _e: self.do_history())
        self._field(row, "Số ngày gần nhất:", self.v_h_days, 5, "spin", lo=1, hi=60)
        self._field(row, "Loại sự kiện:", self.v_h_type, 12, "combo", values=["Tất cả"] + sorted(EVENT_TYPES))
        self._field(row, "Số dòng tối đa:", self.v_h_limit, 6, "spin", lo=1, hi=1000)
        ttk.Button(row, text="Truy vấn", command=self.do_history).pack(side="left")
        self.l_history = ttk.Label(tab, text="Nguồn: bảng events_by_user.  Nhấp đúp vào một dòng để xem hành trình của phiên đó.",
                                   style="Hint.TLabel")
        self.l_history.pack(anchor="w", pady=(0, 8))
        self.t_history = self._tree(tab, [("time", "Thời gian", 150, "center"), ("type", "Sự kiện", 100, "center"),
                                          ("detail", "Chi tiết", 300, "w"), ("url", "Trang", 190, "w"),
                                          ("device", "Thiết bị", 80, "center"), ("session", "Session ID", 290, "w")])
        self.t_history.bind("<Double-1>", self._history_open_session)

    @guarded
    def do_history(self):
        user = self.v_h_user.get().strip()
        if not user:
            raise ValueError("Vui lòng nhập mã khách hàng, ví dụ U0001.")
        days = self._read_int(self.v_h_days, "Số ngày", 1, 60)
        limit = self._read_int(self.v_h_limit, "Số dòng tối đa", 1, 1000)
        etype = self.v_h_type.get()
        etype = None if etype == "Tất cả" else etype

        def done(rows):
            self._fill(self.t_history, [(fmt_time(r.event_time), r.event_type, describe(r), r.page_url,
                                         r.device, str(r.session_id)) for r in rows])
            self.l_history.configure(text=f"{user}: {len(rows)} sự kiện (mới nhất trước).  "
                                          "Nhấp đúp vào một dòng để xem hành trình của phiên đó.")

        self.run_async(lambda: self.svc.user_history(user, days, etype, limit), done)

    def _history_open_session(self, _event):
        selected = self.t_history.selection()
        if not selected:
            return
        self.v_s_id.set(self.t_history.item(selected[0], "values")[5])
        self.nb.select(self.tab_session)
        self.do_session()

    # =====================================================================
    # Chức năng 3: hành trình phiên (Cassandra)
    # =====================================================================
    def _tab_session(self):
        tab = self.tab_session = self._tab("3. Hành trình phiên")
        self.v_s_id = tk.StringVar()
        row = self._row(tab)
        entry = self._field(row, "Session ID:", self.v_s_id, 48)
        entry.bind("<Return>", lambda _e: self.do_session())
        ttk.Button(row, text="Truy vấn", command=self.do_session).pack(side="left")
        self.l_session = ttk.Label(tab, text="Nguồn: bảng events_by_session.  Dán Session ID lấy từ tab 2, hoặc nhấp đúp ở tab 2.",
                                   style="Hint.TLabel")
        self.l_session.pack(anchor="w", pady=(0, 8))
        self.t_session = self._tree(tab, [("n", "#", 50, "center"), ("time", "Thời gian", 160, "center"),
                                          ("type", "Sự kiện", 110, "center"), ("detail", "Chi tiết", 360, "w"),
                                          ("url", "Trang", 240, "w")])

    @guarded
    def do_session(self):
        try:
            sid = uuid.UUID(self.v_s_id.get().strip())
        except ValueError:
            raise ValueError("Session ID không đúng định dạng UUID. Hãy sao chép từ kết quả ở tab 2.") from None

        def done(rows):
            if not rows:
                self._fill(self.t_session, [])
                self.l_session.configure(text="Không tìm thấy phiên này.")
                return
            seconds = int((rows[-1].event_time - rows[0].event_time).total_seconds())
            self.l_session.configure(text=f"Khách hàng {rows[0].user_id}  |  thiết bị {rows[0].device}  |  "
                                          f"{len(rows)} sự kiện  |  kéo dài {seconds} giây")
            self._fill(self.t_session, [(i, fmt_time(r.event_time), r.event_type, describe(r), r.page_url)
                                        for i, r in enumerate(rows, 1)])

        self.run_async(lambda: self.svc.session_path(sid), done)

    # =====================================================================
    # Chức năng 4: thống kê sản phẩm theo ngày (Cassandra)
    # =====================================================================
    def _tab_products(self):
        tab = self._tab("4. Thống kê sản phẩm")
        self.v_p_day = tk.StringVar(value=str(today_vn()))
        self.v_p_top = tk.StringVar(value="10")
        row = self._row(tab)
        entry = self._field(row, "Ngày (YYYY-MM-DD):", self.v_p_day, 12)
        entry.bind("<Return>", lambda _e: self.do_products())
        self._field(row, "Hiển thị top:", self.v_p_top, 5, "spin", lo=1, hi=100)
        ttk.Button(row, text="Truy vấn", command=self.do_products).pack(side="left")
        ttk.Label(tab, text="Nguồn: bảng counter product_stats_by_day.  Nhấp vào tiêu đề cột để sắp xếp.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 8))
        self.t_products = self._tree(tab, [("rank", "Hạng", 60, "center"), ("pid", "Mã SP", 80, "center"),
                                           ("name", "Sản phẩm", 320, "w"), ("view", "View", 90, "e"),
                                           ("click", "Click", 90, "e"), ("cart", "Giỏ hàng", 100, "e"),
                                           ("rate", "Giỏ/View", 100, "e")], height=8)
        self.c_products = BarChart(tab)
        self.c_products.pack(fill="x", pady=(10, 0))

    @guarded
    def do_products(self):
        day = self._read_day(self.v_p_day)
        top = self._read_int(self.v_p_top, "Số lượng top", 1, 100)

        def done(stats):
            ranked = sorted(stats.items(), key=lambda kv: kv[1]["view"], reverse=True)[:top]
            rows = []
            for rank, (pid, c) in enumerate(ranked, 1):
                rate = f"{c['add_to_cart'] / c['view'] * 100:.1f}%" if c["view"] else "-"
                rows.append((rank, pid, product_name(pid), f"{c['view']:,}", f"{c['click']:,}",
                             f"{c['add_to_cart']:,}", rate))
            self._fill(self.t_products, rows)
            self.c_products.set_data([(product_name(p), c["view"]) for p, c in ranked[:10]],
                                     f"Lượt xem theo sản phẩm - ngày {day}" if ranked else "")
            if not ranked:
                messagebox.showinfo("Không có dữ liệu", f"Không có dữ liệu thống kê cho ngày {day}.", parent=self)

        self.run_async(lambda: self.svc.product_stats(day), done)

    # =====================================================================
    # Chức năng 5: từ khóa tìm kiếm (Cassandra)
    # =====================================================================
    def _tab_keywords(self):
        tab = self._tab("5. Từ khóa tìm kiếm")
        self.v_k_day = tk.StringVar(value=str(today_vn()))
        self.v_k_top = tk.StringVar(value="10")
        row = self._row(tab)
        entry = self._field(row, "Ngày (YYYY-MM-DD):", self.v_k_day, 12)
        entry.bind("<Return>", lambda _e: self.do_keywords())
        self._field(row, "Hiển thị top:", self.v_k_top, 5, "spin", lo=1, hi=100)
        ttk.Button(row, text="Truy vấn", command=self.do_keywords).pack(side="left")
        ttk.Label(tab, text="Nguồn: bảng counter keyword_stats_by_day.", style="Hint.TLabel").pack(anchor="w", pady=(0, 8))
        self.t_keywords = self._tree(tab, [("rank", "Hạng", 60, "center"), ("kw", "Từ khóa", 360, "w"),
                                           ("cnt", "Số lượt tìm", 120, "e")], height=8)
        self.c_keywords = BarChart(tab, color="#D85A30")
        self.c_keywords.pack(fill="x", pady=(10, 0))

    @guarded
    def do_keywords(self):
        day = self._read_day(self.v_k_day)
        top = self._read_int(self.v_k_top, "Số lượng top", 1, 100)

        def done(rows):
            rows = rows[:top]
            self._fill(self.t_keywords, [(i, kw, f"{cnt:,}") for i, (kw, cnt) in enumerate(rows, 1)])
            self.c_keywords.set_data(rows[:10], f"Từ khóa tìm kiếm phổ biến - ngày {day}" if rows else "")
            if not rows:
                messagebox.showinfo("Không có dữ liệu", f"Không có dữ liệu từ khóa cho ngày {day}.", parent=self)

        self.run_async(lambda: self.svc.keyword_stats(day), done)

    # =====================================================================
    # Chức năng 6: tổng quan số sự kiện (Cassandra, từ các bảng counter)
    # =====================================================================
    def _tab_overview(self):
        tab = self._tab("6. Tổng quan sự kiện")
        self.v_o_days = tk.StringVar(value="8")
        row = self._row(tab)
        self._field(row, "Số ngày gần nhất:", self.v_o_days, 5, "spin", lo=1, hi=60)
        ttk.Button(row, text="Truy vấn", command=self.do_overview).pack(side="left")
        ttk.Label(tab, text="Cộng từ các bảng counter, kết quả tức thì. Dữ liệu rải 7 ngày thì nhập 8 để bao đủ các ngày.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 8))
        self.t_overview = self._tree(tab, [("day", "Ngày", 130, "center"), ("click", "click", 110, "e"),
                                           ("view", "view", 110, "e"), ("cart", "add_to_cart", 120, "e"),
                                           ("search", "search", 110, "e"), ("total", "Tổng", 130, "e")], height=9)
        self.c_overview = BarChart(tab, color="#378ADD")
        self.c_overview.pack(fill="x", pady=(10, 0))

    @guarded
    def do_overview(self):
        days = self._read_int(self.v_o_days, "Số ngày", 1, 60)

        def done(result):
            rows, grand, chart = [], defaultdict(int), []
            for day, c in result:
                total = sum(c.values())
                if total == 0:
                    continue
                rows.append((str(day), f"{c['click']:,}", f"{c['view']:,}", f"{c['add_to_cart']:,}",
                             f"{c['search']:,}", f"{total:,}"))
                chart.append((str(day), total))
                for k, v in c.items():
                    grand[k] += v
            if rows:
                rows.append(("TỔNG", f"{grand['click']:,}", f"{grand['view']:,}", f"{grand['add_to_cart']:,}",
                             f"{grand['search']:,}", f"{sum(grand.values()):,}"))
            self._fill(self.t_overview, rows, total_last=True)
            self.c_overview.set_data(chart, f"Tổng số sự kiện theo ngày (tổng cộng {sum(grand.values()):,})" if rows else "")

        self.run_async(lambda: self.svc.overview(days), done)

    # =====================================================================
    # Chức năng 7: đếm chính xác số hàng events_by_user (Cassandra)
    # =====================================================================
    def _tab_count(self):
        tab = self._tab("7. Đếm chính xác số hàng")
        self.v_c_users = tk.StringVar(value="1000")
        self.v_c_days = tk.StringVar(value="8")
        row = self._row(tab)
        self._field(row, "Số user đã giả lập:", self.v_c_users, 8, "spin", lo=1, hi=9999)
        self._field(row, "Số ngày cần quét:", self.v_c_days, 5, "spin", lo=1, hi=60)
        ttk.Button(row, text="Đếm", command=self.do_count).pack(side="left")
        ttk.Label(tab, text="Đếm từng partition (user, ngày) của bảng events_by_user rồi cộng lại, "
                            "tránh COUNT(*) toàn bảng dễ bị timeout.", style="Hint.TLabel").pack(anchor="w", pady=(0, 24))
        self.l_count = ttk.Label(tab, text="—", style="Big.TLabel")
        self.l_count.pack(anchor="w")
        self.l_count_note = ttk.Label(tab, text="", style="Hint.TLabel")
        self.l_count_note.pack(anchor="w", pady=(6, 0))

    @guarded
    def do_count(self):
        users = self._read_int(self.v_c_users, "Số user", 1, 9999)
        days = self._read_int(self.v_c_days, "Số ngày", 1, 60)
        self.l_count.configure(text="Đang đếm...")
        self.l_count_note.configure(text="")

        def done(res):
            total, partitions = res
            self.l_count.configure(text=f"{total:,} hàng")
            self.l_count_note.configure(
                text=f"events_by_user (quét {partitions:,} partition).  events_by_session lưu cùng số hàng này.")

        def failed(err):
            self.l_count.configure(text="—")
            self._default_error(err)

        self.run_async(lambda: self.svc.exact_count(users, days), done, failed)

    # =====================================================================
    # Chức năng 8: xu hướng tương tác của một sản phẩm theo ngày (Cassandra)
    # =====================================================================
    def _tab_trend(self):
        tab = self._tab("8. Xu hướng SP")
        choices = [f"{pid} - {info[0]}" for pid, info in PRODUCTS.items()]
        self.v_t_prod = tk.StringVar(value=choices[0])
        self.v_t_days = tk.StringVar(value="8")
        row = self._row(tab)
        self._field(row, "Sản phẩm:", self.v_t_prod, 34, "combo", values=choices)
        self._field(row, "Số ngày gần nhất:", self.v_t_days, 5, "spin", lo=1, hi=60)
        ttk.Button(row, text="Truy vấn", command=self.do_trend).pack(side="left")
        ttk.Label(tab, text="Nguồn: bảng counter product_stats_by_day (khóa event_date + product_id).  "
                            "Dữ liệu rải 7 ngày thì nhập 8 để bao đủ các ngày.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 8))
        self.t_trend = self._tree(tab, [("day", "Ngày", 130, "center"), ("view", "View", 100, "e"),
                                        ("click", "Click", 100, "e"), ("cart", "Giỏ hàng", 110, "e"),
                                        ("rate", "Giỏ/View", 110, "e")], height=9)
        self.c_trend = BarChart(tab, color="#7F77DD")
        self.c_trend.pack(fill="x", pady=(10, 0))

    @guarded
    def do_trend(self):
        pid = self.v_t_prod.get().split(" - ")[0].strip()
        if pid not in PRODUCTS:
            raise ValueError("Vui lòng chọn một sản phẩm trong danh sách.")
        days = self._read_int(self.v_t_days, "Số ngày", 1, 60)

        def done(result):
            rows, grand, chart = [], defaultdict(int), []
            for day, c in result:
                if not any(c.values()):
                    continue
                rate = f"{c['add_to_cart'] / c['view'] * 100:.1f}%" if c["view"] else "-"
                rows.append((str(day), f"{c['view']:,}", f"{c['click']:,}", f"{c['add_to_cart']:,}", rate))
                chart.append((str(day), c["view"]))
                for k, v in c.items():
                    grand[k] += v
            if rows:
                rate = f"{grand['add_to_cart'] / grand['view'] * 100:.1f}%" if grand["view"] else "-"
                rows.append(("TỔNG", f"{grand['view']:,}", f"{grand['click']:,}",
                             f"{grand['add_to_cart']:,}", rate))
            self._fill(self.t_trend, rows, total_last=True)
            self.c_trend.set_data(chart, f"Lượt xem {product_name(pid)} theo ngày" if rows else "")
            if not rows:
                messagebox.showinfo("Không có dữ liệu",
                                    f"Không có dữ liệu của sản phẩm {pid} trong {days} ngày gần nhất.", parent=self)

        self.run_async(lambda: self.svc.product_trend(pid, days), done)

    # =====================================================================
    # Chức năng 9: các phiên truy cập của một khách hàng trong ngày (Cassandra)
    # =====================================================================
    def _tab_sessions(self):
        tab = self._tab("9. Phiên khách hàng")
        self.v_u_user = tk.StringVar(value="U0001")
        self.v_u_day = tk.StringVar(value=str(today_vn()))
        row = self._row(tab)
        entry = self._field(row, "Mã khách hàng:", self.v_u_user, 12)
        entry.bind("<Return>", lambda _e: self.do_sessions())
        entry = self._field(row, "Ngày (YYYY-MM-DD):", self.v_u_day, 12)
        entry.bind("<Return>", lambda _e: self.do_sessions())
        ttk.Button(row, text="Truy vấn", command=self.do_sessions).pack(side="left")
        self.l_sessions = ttk.Label(tab, text="Nguồn: bảng events_by_user (khóa user_id + event_date), gom theo session_id.  "
                                              "Nhấp đúp vào một dòng để xem hành trình của phiên đó.",
                                    style="Hint.TLabel")
        self.l_sessions.pack(anchor="w", pady=(0, 8))
        self.t_sessions = self._tree(tab, [("start", "Bắt đầu", 160, "center"), ("sec", "Kéo dài (giây)", 120, "e"),
                                           ("events", "Số sự kiện", 100, "e"), ("views", "View", 70, "e"),
                                           ("carts", "Giỏ hàng", 90, "e"), ("device", "Thiết bị", 80, "center"),
                                           ("session", "Session ID", 290, "w")])
        self.t_sessions.bind("<Double-1>", self._sessions_open_session)

    @guarded
    def do_sessions(self):
        user = self.v_u_user.get().strip()
        if not user:
            raise ValueError("Vui lòng nhập mã khách hàng, ví dụ U0001.")
        day = self._read_day(self.v_u_day)

        def done(sessions):
            self._fill(self.t_sessions, [(fmt_time(s["start"]), f"{s['seconds']:,}", s["events"], s["views"],
                                          s["carts"], s["device"], str(s["session_id"])) for s in sessions])
            self.l_sessions.configure(
                text=(f"{user} ngày {day}: {len(sessions)} phiên (mới nhất trước).  "
                      "Nhấp đúp vào một dòng để xem hành trình của phiên đó.")
                if sessions else f"Không có dữ liệu của khách hàng {user} trong ngày {day}.")

        self.run_async(lambda: self.svc.user_sessions(user, day), done)

    def _sessions_open_session(self, _event):
        selected = self.t_sessions.selection()
        if not selected:
            return
        self.v_s_id.set(self.t_sessions.item(selected[0], "values")[6])
        self.nb.select(self.tab_session)
        self.do_session()


def main():
    try:  # chữ sắc nét trên màn hình độ phân giải cao (Windows)
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:  # noqa: BLE001
        pass
    App().mainloop()


if __name__ == "__main__":
    main()
