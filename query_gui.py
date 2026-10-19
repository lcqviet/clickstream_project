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
    ROW = 32

    def __init__(self, master, color="#1D9E75"):
        super().__init__(master, background="#FFFFFF", highlightthickness=1,
                         highlightbackground="#DEE2E6", height=80)
        self.color, self.items, self.title = color, [], ""
        self.bind("<Configure>", lambda _e: self._draw())

    def set_data(self, items, title=""):
        self.items, self.title = items, title
        self.configure(height=50 + self.ROW * max(len(items), 1))
        self._draw()

    def _draw(self):
        self.delete("all")
        width = self.winfo_width()
        if width < 120:
            return
        self.create_text(15, 20, text=self.title, anchor="w", font=FONT_BOLD, fill="#212529")
        if not self.items:
            self.create_text(15, 55, text="(không có dữ liệu)", anchor="w", font=FONT, fill="#6C757D")
            return
        label_w = 260
        top = max(v for _, v in self.items) or 1
        bar_max = max(width - label_w - 120, 60)
        for i, (label, value) in enumerate(self.items):
            y = 45 + i * self.ROW
            text = label if len(label) <= 36 else label[:35] + "…"
            self.create_text(label_w - 12, y + 16, text=text, anchor="e", font=FONT, fill="#212529")
            bar = max(2, int(bar_max * value / top))
            self.create_rectangle(label_w, y + 4, label_w + bar, y + 26, fill=self.color, outline="", width=0)
            self.create_text(label_w + bar + 10, y + 16, text=f"{value:,}", anchor="w", font=FONT, fill="#212529")


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
        self.geometry("1280x800")
        self.minsize(1024, 720)
        self.configure(bg="#FFFFFF")
        self.svc = None
        self._jobs = queue.Queue()

        self._setup_style()
        self._build_header()
        self._build_status()  
        
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=12, pady=(12, 0))
        for build in (self._tab_recent, self._tab_history, self._tab_session, self._tab_products,
                      self._tab_keywords, self._tab_overview, self._tab_count,
                      self._tab_trend, self._tab_sessions):
            build()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(100, self._poll)
        self._connect()

    # ----- Hàm sinh danh sách User -----
    def get_user_list(self):
        """Hàm sinh danh sách mã khách hàng gợi ý, gọi độc lập ở các tính năng cần thiết."""
        return [f"U{i:04d}" for i in range(1, 1001)]

    # ----- giao diện chung -----
    def _setup_style(self):
        style = ttk.Style(self)
        for theme in ("clam", "vista"):
            if theme in style.theme_names():
                style.theme_use(theme)
                break
                
        bg_color = "#FFFFFF"
        alt_bg = "#F8F9FA"
        fg_color = "#212529"
        
        style.configure(".", font=FONT, background=bg_color, foreground=fg_color)
        style.configure("TFrame", background=bg_color)
        
        style.configure("Treeview", rowheight=32, font=FONT, borderwidth=0, background=bg_color, fieldbackground=bg_color)
        style.configure("Treeview.Heading", font=FONT_BOLD, background="#E9ECEF", foreground=fg_color, padding=6)
        style.map("Treeview", background=[("selected", TEAL)], foreground=[("selected", "white")])
        
        style.configure("TNotebook", background=alt_bg, borderwidth=0)
        style.configure("TNotebook.Tab", padding=(16, 10), font=FONT_BOLD, background="#DEE2E6")
        style.map("TNotebook.Tab", background=[("selected", bg_color)], foreground=[("selected", TEAL)])
        
        style.configure("Big.TLabel", font=("Segoe UI", 24, "bold"), foreground=TEAL, background=bg_color)
        style.configure("Hint.TLabel", foreground="#6C757D", font=("Segoe UI", 9, "italic"), background=bg_color)
        style.configure("TButton", font=FONT_BOLD, padding=5)

    def _build_header(self):
        bar = tk.Frame(self, background=TEAL, height=80)
        bar.pack(fill="x")
        tk.Label(bar, text="HỆ THỐNG LƯU VẾT HÀNH VI NGƯỜI DÙNG", bg=TEAL, fg="white",
                 font=("Segoe UI", 16, "bold")).pack(anchor="w", padx=20, pady=(15, 2))
        tk.Label(bar, text="Trình quản lý & Truy vấn Clickstream  |  Cassandra (log đầy đủ) & Redis (truy xuất nhanh)",
                 bg=TEAL, fg="#CFEDE2", font=("Segoe UI", 11)).pack(anchor="w", padx=20, pady=(0, 15))
        ttk.Separator(self).pack(fill="x")

    def _build_status(self):
        bar = ttk.Frame(self, padding=(15, 5))
        bar.pack(side="bottom", fill="x")
        self.status = ttk.Label(bar, text="", font=("Segoe UI", 9))
        self.status.pack(side="left")
        self.reconnect_btn = ttk.Button(bar, text="Kết nối lại", command=self._connect)
        self.progress = ttk.Progressbar(bar, mode="indeterminate", length=180)
        self.progress.pack(side="right")

    def set_status(self, text, busy=False, error=False):
        self.status.configure(text=text, foreground="#B42318" if error else "#24292F")
        if busy:
            self.progress.start(12)
        else:
            self.progress.stop()
            
    def _update_status(self, text):
        self.set_status(text, busy=True)

    def run_async(self, work, done, on_error=None):
        if self.svc is None:
            messagebox.showwarning("Chưa kết nối", "Chương trình chưa kết nối được tới cơ sở dữ liệu.", parent=self)
            return
        self.set_status("Đang truy vấn...", busy=True)

        def target():
            start = time.time()
            attempts = 3
            for attempt in range(1, attempts + 1):
                try:
                    result = work()
                    self._jobs.put((done, on_error, result, None, time.time() - start))
                    return
                except BaseException as err:
                    err_name = type(err).__name__
                    if attempt < attempts and err_name in ('ReadTimeout', 'OperationTimedOut', 'Timeout'):
                        self._jobs.put((self._update_status, None, f"Hệ thống đang chịu tải nặng, thử lại ({attempt}/{attempts})...", None, 0))
                        time.sleep(2) 
                        continue
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

    def _connect(self):
        self.reconnect_btn.pack_forget()
        self.set_status("Đang kết nối Cassandra và Redis (tối đa khoảng 60 giây nếu Docker vừa khởi động)...", busy=True)

        def target():
            try:
                self._jobs.put((self._on_connected, self._on_connect_failed, ClickstreamQuery(), None, 0))
            except BaseException as err:  
                self._jobs.put((self._on_connected, self._on_connect_failed, None, err, 0))

        threading.Thread(target=target, daemon=True).start()

    def _on_connected(self, svc):
        self.svc = svc
        try:
            if hasattr(self.svc, 'session'):
                self.svc.session.default_timeout = 60.0
            elif hasattr(self.svc, 'cass'):
                self.svc.cass.default_timeout = 60.0
        except Exception:
            pass
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
        frame = ttk.Frame(self.nb, padding=16)
        self.nb.add(frame, text=title)
        return frame

    @staticmethod
    def _row(parent):
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=(0, 16))
        return row

    @staticmethod
    def _field(row, label, var, width=15, kind="entry", values=None, lo=1, hi=999999):
        """Hỗ trợ kind='user' (Combobox có thể nhập tay) và kind='combo' (Combobox chỉ đọc)"""
        ttk.Label(row, text=label, font=FONT_BOLD).pack(side="left", padx=(0, 8))
        
        if kind == "combo":
            w = ttk.Combobox(row, textvariable=var, values=values, width=width, state="readonly", font=FONT)
        elif kind == "user":
            w = ttk.Combobox(row, textvariable=var, values=values, width=width, font=FONT)
        elif kind == "spin":
            w = ttk.Spinbox(row, textvariable=var, from_=lo, to=hi, width=width, font=FONT)
        else:
            w = ttk.Entry(row, textvariable=var, width=width, font=FONT)
            
        w.pack(side="left", padx=(0, 24))
        return w

    def _tree(self, parent, columns, expand=True, height=12):
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
        tree.tag_configure("odd", background="#F8F9FA")
        tree.tag_configure("even", background="#FFFFFF")
        tree.tag_configure("total", font=FONT_BOLD, background="#DCEFE8")
        return tree

    @staticmethod
    def _fill(tree, rows, total_last=False):
        tree.delete(*tree.get_children())
        for i, row in enumerate(rows):
            tag = "total" if total_last and i == len(rows) - 1 else ("odd" if i % 2 else "even")
            tree.insert("", "end", values=row, tags=(tag,))

    @staticmethod
    def _sort(tree, col):
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
        tab = self._tab("1. SP Vừa Xem (Redis)")
        self.v_recent_user = tk.StringVar(value="U0001")
        row = self._row(tab)
        
        # Truyền danh sách user vào ô Combobox bằng hàm get_user_list
        users = self.get_user_list()
        entry = self._field(row, "Mã khách hàng:", self.v_recent_user, 15, kind="user", values=users)
        entry.bind("<Return>", lambda _e: self.do_recent())
        
        ttk.Button(row, text="Truy vấn", command=self.do_recent).pack(side="left", padx=10)
        
        self.l_recent = ttk.Label(tab, text="Nguồn: Redis Sorted Set user:{id}:recent_views (ZREVRANGE 0 4)", style="Hint.TLabel")
        self.l_recent.pack(anchor="w", pady=(0, 10))
        
        self.t_recent = self._tree(tab, [("rank", "Hạng", 70, "center"), ("pid", "Mã SP", 100, "center"),
                                         ("name", "Tên sản phẩm", 400, "w"), ("when", "Thời điểm xem gần nhất", 220, "center")])

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
        tab = self._tab("2. Lịch Sử Hành Vi")
        self.v_h_user = tk.StringVar(value="U0001")
        self.v_h_days = tk.StringVar(value="1")
        self.v_h_type = tk.StringVar(value="Tất cả")
        self.v_h_limit = tk.StringVar(value="50")
        
        row = self._row(tab)
        # Truyền danh sách user vào ô Combobox
        users = self.get_user_list()
        entry = self._field(row, "Mã khách hàng:", self.v_h_user, 15, kind="user", values=users)
        entry.bind("<Return>", lambda _e: self.do_history())
        
        self._field(row, "Số ngày gần nhất:", self.v_h_days, 5, "spin", lo=1, hi=60)
        self._field(row, "Loại sự kiện:", self.v_h_type, 12, "combo", values=["Tất cả"] + sorted(EVENT_TYPES))
        self._field(row, "Số dòng tối đa:", self.v_h_limit, 6, "spin", lo=1, hi=1000)
        
        ttk.Button(row, text="Truy vấn", command=self.do_history).pack(side="left", padx=10)
        
        self.l_history = ttk.Label(tab, text="Nguồn: bảng events_by_user. Nhấp đúp vào một dòng để xem hành trình của phiên đó.",
                                   style="Hint.TLabel")
        self.l_history.pack(anchor="w", pady=(0, 10))
        
        self.t_history = self._tree(tab, [("time", "Thời gian", 160, "center"), ("type", "Sự kiện", 110, "center"),
                                          ("detail", "Chi tiết", 320, "w"), ("url", "Trang", 210, "w"),
                                          ("device", "Thiết bị", 90, "center"), ("session", "Session ID", 300, "w")])
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
        tab = self.tab_session = self._tab("3. Hành Trình Phiên")
        self.v_s_id = tk.StringVar()
        
        row = self._row(tab)
        entry = self._field(row, "Session ID:", self.v_s_id, 45)
        entry.bind("<Return>", lambda _e: self.do_session())
        ttk.Button(row, text="Truy vấn", command=self.do_session).pack(side="left", padx=10)
        
        self.l_session = ttk.Label(tab, text="Nguồn: bảng events_by_session. Dán Session ID lấy từ tab 2, hoặc nhấp đúp ở tab 2.",
                                   style="Hint.TLabel")
        self.l_session.pack(anchor="w", pady=(0, 10))
        
        self.t_session = self._tree(tab, [("n", "#", 50, "center"), ("time", "Thời gian", 170, "center"),
                                          ("type", "Sự kiện", 120, "center"), ("detail", "Chi tiết", 380, "w"),
                                          ("url", "Trang", 260, "w")])

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
            self.l_session.configure(text=f"Khách hàng {rows[0].user_id}  |  Thiết bị: {rows[0].device}  |  "
                                          f"{len(rows)} sự kiện  |  Kéo dài {seconds} giây")
            self._fill(self.t_session, [(i, fmt_time(r.event_time), r.event_type, describe(r), r.page_url)
                                        for i, r in enumerate(rows, 1)])

        self.run_async(lambda: self.svc.session_path(sid), done)

    # =====================================================================
    # Chức năng 4: thống kê sản phẩm theo ngày (Cassandra)
    # =====================================================================
    def _tab_products(self):
        tab = self._tab("4. Thống Kê Sản Phẩm")
        self.v_p_day = tk.StringVar(value=str(today_vn()))
        self.v_p_top = tk.StringVar(value="10")
        
        row = self._row(tab)
        # Sử dụng ô nhập liệu chuẩn
        self._field(row, "Ngày (YYYY-MM-DD):", self.v_p_day, 14, kind="entry")
        self._field(row, "Hiển thị top:", self.v_p_top, 6, "spin", lo=1, hi=100)
        ttk.Button(row, text="Truy vấn", command=self.do_products).pack(side="left", padx=10)
        
        ttk.Label(tab, text="Nguồn: bảng counter product_stats_by_day. Nhấp vào tiêu đề cột để sắp xếp.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 10))
                  
        self.t_products = self._tree(tab, [("rank", "Hạng", 70, "center"), ("pid", "Mã SP", 90, "center"),
                                           ("name", "Sản phẩm", 340, "w"), ("view", "View", 100, "e"),
                                           ("click", "Click", 100, "e"), ("cart", "Giỏ hàng", 110, "e"),
                                           ("rate", "Giỏ/View", 110, "e")], height=7)
        self.c_products = BarChart(tab)
        self.c_products.pack(fill="x", pady=(15, 0))

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
        tab = self._tab("5. Từ Khóa Tìm Kiếm")
        self.v_k_day = tk.StringVar(value=str(today_vn()))
        self.v_k_top = tk.StringVar(value="10")
        
        row = self._row(tab)
        self._field(row, "Ngày (YYYY-MM-DD):", self.v_k_day, 14, kind="entry")
        self._field(row, "Hiển thị top:", self.v_k_top, 6, "spin", lo=1, hi=100)
        ttk.Button(row, text="Truy vấn", command=self.do_keywords).pack(side="left", padx=10)
        
        ttk.Label(tab, text="Nguồn: bảng counter keyword_stats_by_day.", style="Hint.TLabel").pack(anchor="w", pady=(0, 10))
        self.t_keywords = self._tree(tab, [("rank", "Hạng", 70, "center"), ("kw", "Từ khóa", 380, "w"),
                                           ("cnt", "Số lượt tìm", 140, "e")], height=7)
        self.c_keywords = BarChart(tab, color="#D85A30")
        self.c_keywords.pack(fill="x", pady=(15, 0))

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
    # Chức năng 6: tổng quan số sự kiện (Cassandra)
    # =====================================================================
    def _tab_overview(self):
        tab = self._tab("6. Tổng Quan Sự Kiện")
        self.v_o_days = tk.StringVar(value="8")
        
        row = self._row(tab)
        self._field(row, "Số ngày gần nhất:", self.v_o_days, 6, "spin", lo=1, hi=60)
        ttk.Button(row, text="Truy vấn", command=self.do_overview).pack(side="left", padx=10)
        
        ttk.Label(tab, text="Cộng từ các bảng counter, kết quả tức thì. Dữ liệu rải 7 ngày thì nhập 8 để bao đủ các ngày.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 10))
        self.t_overview = self._tree(tab, [("day", "Ngày", 140, "center"), ("click", "click", 120, "e"),
                                           ("view", "view", 120, "e"), ("cart", "add_to_cart", 130, "e"),
                                           ("search", "search", 120, "e"), ("total", "Tổng", 140, "e")], height=8)
        self.c_overview = BarChart(tab, color="#378ADD")
        self.c_overview.pack(fill="x", pady=(15, 0))

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
        tab = self._tab("7. Đếm Chính Xác Hàng")
        self.v_c_users = tk.StringVar(value="1000")
        self.v_c_days = tk.StringVar(value="8")
        
        row = self._row(tab)
        self._field(row, "Số user đã giả lập:", self.v_c_users, 8, "spin", lo=1, hi=9999)
        self._field(row, "Số ngày cần quét:", self.v_c_days, 6, "spin", lo=1, hi=60)
        ttk.Button(row, text="Đếm", command=self.do_count).pack(side="left", padx=10)
        
        #ttk.Label(tab, text="Đếm từng partition (user, ngày) của bảng events_by_user rồi cộng lại, "
        #                    "tránh COUNT(*) toàn bảng dễ bị timeout.", style="Hint.TLabel").pack(anchor="w", pady=(0, 30))
                            
        self.l_count = ttk.Label(tab, text="—", style="Big.TLabel")
        self.l_count.pack(anchor="w", pady=(10, 0))
        self.l_count_note = ttk.Label(tab, text="", style="Hint.TLabel")
        self.l_count_note.pack(anchor="w", pady=(10, 0))

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
                text=f"events_by_user (quét {partitions:,} partition). events_by_session lưu cùng số hàng này.")

        def failed(err):
            self.l_count.configure(text="—")
            self._default_error(err)

        self.run_async(lambda: self.svc.exact_count(users, days), done, failed)

    # =====================================================================
    # Chức năng 8: xu hướng tương tác của một sản phẩm theo ngày (Cassandra)
    # =====================================================================
    def _tab_trend(self):
        tab = self._tab("8. Xu Hướng SP")
        choices = [f"{pid} - {info[0]}" for pid, info in PRODUCTS.items()]
        self.v_t_prod = tk.StringVar(value=choices[0])
        self.v_t_days = tk.StringVar(value="8")
        
        row = self._row(tab)
        self._field(row, "Sản phẩm:", self.v_t_prod, 38, "combo", values=choices)
        self._field(row, "Số ngày gần nhất:", self.v_t_days, 6, "spin", lo=1, hi=60)
        ttk.Button(row, text="Truy vấn", command=self.do_trend).pack(side="left", padx=10)
        
        ttk.Label(tab, text="Nguồn: bảng counter product_stats_by_day (khóa event_date + product_id). "
                            "Dữ liệu rải 7 ngày thì nhập 8 để bao đủ.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 10))
                  
        self.t_trend = self._tree(tab, [("day", "Ngày", 140, "center"), ("view", "View", 110, "e"),
                                        ("click", "Click", 110, "e"), ("cart", "Giỏ hàng", 120, "e"),
                                        ("rate", "Giỏ/View", 120, "e")], height=8)
        self.c_trend = BarChart(tab, color="#7F77DD")
        self.c_trend.pack(fill="x", pady=(15, 0))

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
    # Chức năng 9: các phiên truy cập của một khách hàng trong ngày
    # =====================================================================
    def _tab_sessions(self):
        tab = self._tab("9. Phiên Khách Hàng")
        self.v_u_user = tk.StringVar(value="U0001")
        self.v_u_day = tk.StringVar(value=str(today_vn()))
        
        row = self._row(tab)
        # Truyền danh sách user vào ô Combobox
        users = self.get_user_list()
        entry1 = self._field(row, "Mã khách hàng:", self.v_u_user, 15, kind="user", values=users)
        entry1.bind("<Return>", lambda _e: self.do_sessions())
        
        self._field(row, "Ngày (YYYY-MM-DD):", self.v_u_day, 14, kind="entry")
        ttk.Button(row, text="Truy vấn", command=self.do_sessions).pack(side="left", padx=10)
        
        self.l_sessions = ttk.Label(tab, text="Nguồn: bảng events_by_user (khóa user_id + event_date), gom theo session_id. "
                                              "Nhấp đúp vào một dòng để xem hành trình của phiên đó.",
                                    style="Hint.TLabel")
        self.l_sessions.pack(anchor="w", pady=(0, 10))
        
        self.t_sessions = self._tree(tab, [("start", "Bắt đầu", 170, "center"), ("sec", "Kéo dài (giây)", 130, "e"),
                                           ("events", "Số sự kiện", 110, "e"), ("views", "View", 80, "e"),
                                           ("carts", "Giỏ hàng", 100, "e"), ("device", "Thiết bị", 90, "center"),
                                           ("session", "Session ID", 310, "w")])
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
    try:  
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception: 
        pass
    App().mainloop()


if __name__ == "__main__":
    main()