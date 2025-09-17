import customtkinter as ctk
from tkinter import messagebox
import threading
import time
from datetime import datetime, timedelta
from dateutil import parser
from widgets import CollapsibleSection
from plot_manager import PlotManager
from query_manager import QueryManager
import config_manager
from tksheet import Sheet
from datetime import datetime

class MetricsApp:
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.held_keys = set()
        self.modifiers = set()
        # Core state
        self.df = None
        # Debug flags
        self.enable_plot = True  # turn to False to skip PlotManager
        self.enable_table = True  # turn to False to skip tksheet

        self._timer_after_id = None

        self.plot_manager = None
        self.threads = []
        self.after_ids = []
        self.is_closing = False
        self.query_start_time = None
        self.timer_running = False
        self.table_dragging = False
        self.table_drag_start = None
        self.table_panning = False
        self.table_pan_start = None
        self._table_order = []  # current row order (list of row indices)
        self._sorting = False  # debounce flag
        self._sort_dir = {}  # per-column toggle (True=asc, False=desc)
        self._last_cell = (0, 0)
        # Master color map
        self.color_map = {
            "fan_tach_rpm": "#1E90FF",
            "coolant_temp_f": "#FF4500",
            "ebox_temp_f": "#FFD700",
            "water_temp_f": "#00CED1",
            "target_temp_f": "#ADFF2F",
            "flow_sense_lpm": "#00FF7F",
            "pump_current_amp": "#FF69B4",
            "compressor_current_amp": "#FFA500",
        }

        # Root window
        self.root = ctk.CTk()
        self.root.withdraw()
        self.root.title("Plunge Tub Metrics Analytics")
        self.query_manager = QueryManager(logger=self.log, root=self.root)
        # Load config
        self.config = config_manager.load_config()
        self._saved_col_states = self.config.get("col_states", {})
        menubar = ctk.CTkMenu(self.root) if hasattr(ctk, "CTkMenu") else None
        if menubar is None:
            from tkinter import Menu
            menubar = Menu(self.root)

        file_menu = Menu(menubar, tearoff=0)
        file_menu.add_command(label="Edit Env", command=self.open_env_editor)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.on_close)

        menubar.add_cascade(label="File", menu=file_menu)
        self.root.config(menu=menubar)
        # Build UI
        self.build_output()
        self.build_controls()
        self.build_log()


        # Grid weights
        # Root grid config
        self.root.grid_rowconfigure(0, weight=0)  # controls fixed
        self.root.grid_rowconfigure(1, weight=1)  # output flex
        self.root.grid_rowconfigure(2, weight=0)  # log collapsible
        self.root.grid_columnconfigure(0, weight=1)

        # Restore window size/state
        win_geom = self.config.get("window_size", "1200x950+100+100")
        self.root.geometry(win_geom)

        win_state = self.config.get("window_state", "normal")
        if win_state != "normal":
            self.root.after_idle(lambda: self.root.state(win_state))

        # Restore collapsible states
        states = self.config.get("collapsible_states", {})
        self.filter_date_section.set_state(states.get("filter_date", "expanded"))
        self.table_section.set_state(states.get("table", "expanded"))
        self.log_section.set_state(states.get("log", "expanded"))

        # Protocols/bindings
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<KeyPress>", self.on_key_press)
        self.root.bind("<KeyRelease>", self.on_key_release)
        self.start_date_entry.bind("<FocusOut>", lambda e: self._normalize_entry(self.start_date_entry))
        self.end_date_entry.bind("<FocusOut>", lambda e: self._normalize_entry(self.end_date_entry))

        # Show window
        self.root.after(50, self.root.deiconify)

    def _cancel_all_afters_shutdown(self):
        """Cancel every pending Tk 'after' job just before destroying the root."""
        # 1) cancel our tracked jobs
        for aid in list(self.after_ids):
            try:
                self.root.after_cancel(aid)
            except Exception:
                pass
        self.after_ids.clear()

        # 2) cancel the timer loop if running
        if getattr(self, "_timer_after_id", None):
            try:
                self.root.after_cancel(self._timer_after_id)
            except Exception:
                pass
            self._timer_after_id = None

        # 3) best-effort cancel of ALL remaining Tk jobs (CTk, tksheet, lambdas, etc.)
        try:
            jobs = self.root.tk.call("after", "info")
        except Exception:
            jobs = ()
        for job in jobs:
            try:
                self.root.after_cancel(job)
            except Exception:
                pass

    def open_env_editor(self):
        from env_editor import EnvEditor
        required = [
            "SSH_HOST", "SSH_PORT", "SSH_USER", "SSH_PASSWORD",
            "REMOTE_BIND_HOST", "REMOTE_BIND_PORT",
            "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DB"
        ]
        editor = EnvEditor(self.root, required)
        self.root.wait_window(editor)
        if editor.saved:
            # Tear down any existing connection so the next run reconnects with fresh creds
            try:
                if self.query_manager:
                    self.query_manager.close()  # disposes engine & ssh
                    self.query_manager.engine = None
                    self.query_manager.connector = None  # force a new connector object
            except Exception:
                pass
            self.log("✅ Environment updated. Connection reset; run the query to reconnect.")
        else:
            self.log("⚠️ Environment edit cancelled.")
    # -------------------------------
    # Date parsing + validation
    # -------------------------------
    def _parse_date_str(self, date_str: str) -> datetime:
        """Parse user-entered date, defaulting missing year to current year."""
        today = datetime.today()
        return parser.parse(date_str, default=today)

    def _get_validated_date_range(self) -> tuple[datetime, datetime]:
        """Normalize entries, handle blanks, enforce ordering + max 7-day span."""
        start_str = self.start_date_entry.get().strip()
        end_str = self.end_date_entry.get().strip()

        # If one side is blank, copy from the other
        if start_str and not end_str:
            end_str = start_str
            self.end_date_entry.delete(0, "end")
            self.end_date_entry.insert(0, start_str)
        elif end_str and not start_str:
            start_str = end_str
            self.start_date_entry.delete(0, "end")
            self.start_date_entry.insert(0, end_str)

        # Still both blank? → default to today
        if not start_str and not end_str:
            today = datetime.today().strftime("%Y-%m-%d")
            start_str = end_str = today
            self.start_date_entry.insert(0, today)
            self.end_date_entry.insert(0, today)

        # Normalize both
        start = self._parse_date_str(start_str)
        end = self._parse_date_str(end_str)

        if end <= start:
            raise ValueError("End date must be after start date.")

        if (end - start).days > 7:
            raise ValueError("Date range cannot exceed 7 days.")

        # Write back normalized values
        self.start_date_entry.delete(0, "end")
        self.start_date_entry.insert(0, start.strftime("%Y-%m-%d"))

        self.end_date_entry.delete(0, "end")
        self.end_date_entry.insert(0, end.strftime("%Y-%m-%d"))

        return start, end

    def _normalize_entry(self, entry):
        """Try to normalize entry text to YYYY-MM-DD."""
        raw = entry.get().strip()
        if not raw:
            return
        try:
            dt = self._parse_date_str(raw)
            entry.delete(0, "end")
            entry.insert(0, dt.strftime("%Y-%m-%d"))
        except Exception:
            # don’t overwrite invalid junk, just leave it
            pass

    # -------------------------------
    # Key handling
    # -------------------------------
    def on_key_press(self, event):
        key = event.keysym
        self.held_keys.add(key)

        if key in ("Shift_L", "Shift_R"):
            self.modifiers.add("Shift")
            self.reprocess_held_keys()
        elif key in ("Control_L", "Control_R"):
            self.modifiers.add("Control")
            self.reprocess_held_keys()
        elif key in ("Alt_L", "Alt_R"):
            self.modifiers.add("Alt")
            self.reprocess_held_keys()
        else:
            self.fire_bind(key)

    def on_key_release(self, event):
        key = event.keysym
        self.held_keys.discard(key)

        if key in ("Shift_L", "Shift_R"):
            self.modifiers.discard("Shift")
            self.reprocess_held_keys()
        elif key in ("Control_L", "Control_R"):
            self.modifiers.discard("Control")
            self.reprocess_held_keys()
        elif key in ("Alt_L", "Alt_R"):
            self.modifiers.discard("Alt")
            self.reprocess_held_keys()

    def reprocess_held_keys(self):
        for key in list(self.held_keys):
            if key not in ("Shift_L", "Shift_R", "Control_L", "Control_R", "Alt_L", "Alt_R"):
                self.fire_bind(key)

    def fire_bind(self, key):
        combo = []
        if "Shift" in self.modifiers:
            combo.append("Shift")
        if "Control" in self.modifiers:
            combo.append("Control")
        if "Alt" in self.modifiers:
            combo.append("Alt")
        combo.append(key)

    # -------------------------------
    # UI building
    # -------------------------------
    def build_controls(self):
        # --- Filter, Date & Columns collapsible ---
        self.control_frame = ctk.CTkFrame(self.output_frame, corner_radius=12)
        self.control_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)

        self.filter_date_section = CollapsibleSection(
            self.control_frame, title="Filter, Date & Columns"
        )
        self.filter_date_section.pack(fill="x", padx=10, pady=5)

        # Row container for filter/date/columns
        row_frame = ctk.CTkFrame(self.filter_date_section.content, fg_color="transparent")
        row_frame.pack(fill="x", padx=5, pady=5)

        # ---------------- Filter box ----------------
        filter_box = ctk.CTkFrame(row_frame, corner_radius=8)
        filter_box.pack(side="left", padx=5, pady=5, anchor="n")

        ctk.CTkLabel(filter_box, text="Filter Type:").pack(anchor="w", padx=5, pady=2)
        self.filter_type = ctk.StringVar(value=self.config.get("filter_type", "device_name"))
        self.filter_menu = ctk.CTkComboBox(
            filter_box, variable=self.filter_type,
            values=["device_name", "user_id"], width=180
        )
        self.filter_menu.pack(anchor="w", padx=5, pady=2)

        ctk.CTkLabel(filter_box, text="Filter Value:").pack(anchor="w", padx=5, pady=2)
        self.filter_value = ctk.CTkEntry(filter_box, width=200, placeholder_text="Enter value")
        self.filter_value.insert(0, self.config.get("filter_value", ""))
        self.filter_value.pack(anchor="w", padx=5, pady=2)

        # ---------------- Date box ----------------
        date_box = ctk.CTkFrame(row_frame, corner_radius=8)
        date_box.pack(side="left", padx=5, pady=5, anchor="n")

        ctk.CTkLabel(date_box, text="Start Date:").pack(anchor="w", padx=5, pady=2)
        self.start_date_entry = ctk.CTkEntry(date_box, width=180)
        self.start_date_entry.insert(0, self.config.get(
            "start_date", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        ))
        self.start_date_entry.pack(anchor="w", padx=5, pady=2)

        ctk.CTkLabel(date_box, text="End Date:").pack(anchor="w", padx=5, pady=2)
        self.end_date_entry = ctk.CTkEntry(date_box, width=180)
        self.end_date_entry.insert(0, self.config.get(
            "end_date", datetime.now().strftime("%Y-%m-%d")
        ))
        self.end_date_entry.pack(anchor="w", padx=5, pady=2)

        # ---------------- Columns box ----------------
        # ---------------- Columns box (scrollable) ----------------
        self.col_scroll = ctk.CTkScrollableFrame(
            row_frame,
            corner_radius=8,
            orientation="horizontal",  # ✅ horizontal scrollbar
            height=120  # adjust height to your liking
        )
        self.col_scroll.pack(side="left", fill="x", expand=True, padx=5, pady=5)

        # Two frames for metrics vs others, inside the scrollable area
        self.metrics_col_frame = ctk.CTkFrame(self.col_scroll, corner_radius=8)
        self.metrics_col_frame.pack(side="left", fill="y", padx=5, pady=5)

        self.other_col_frame = ctk.CTkFrame(self.col_scroll, corner_radius=8)
        self.other_col_frame.pack(side="left", fill="y", padx=5, pady=5)

        # Placeholder dict for column checkboxes (rebuilt in show_table)
        self.col_vars = {}

        # Master toggles for later
        self.metrics_toggle = ctk.BooleanVar(value=True)
        self.other_toggle = ctk.BooleanVar(value=True)

    def toggle_metrics(self):
        state = self.metrics_toggle.get()
        for col, var in self.col_vars.items():
            if col in self.color_map:  # metrics only
                var.set(state)
        self.on_column_change()

    def toggle_others(self):
        state = self.other_toggle.get()
        for col, var in self.col_vars.items():
            if col not in self.color_map:  # others only
                var.set(state)
        self.on_column_change()

    def build_output(self):
        self.output_frame = ctk.CTkFrame(self.root, corner_radius=12)
        self.output_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)

        # Configure grid: 4 rows, 1 col
        self.output_frame.grid_rowconfigure(0, weight=1)  # plot flexes
        self.output_frame.grid_rowconfigure(1, weight=0)  # table collapsible
        self.output_frame.grid_rowconfigure(2, weight=0)  # filter/date/columns
        self.output_frame.grid_rowconfigure(3, weight=0)  # run query controls
        self.output_frame.grid_columnconfigure(0, weight=1)

        # ---------------- Plot ----------------
        self.plot_frame = ctk.CTkFrame(self.output_frame, corner_radius=12)
        self.plot_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

        self.plot_manager = PlotManager(
            self.plot_frame,
            on_select=self.on_select,
            on_key=self.on_key
        )

        # ---------------- Data Table ----------------
        self.table_section = CollapsibleSection(self.output_frame, title="Data Table")
        self.table_section.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)

        self.table_frame = ctk.CTkFrame(self.table_section.content, corner_radius=12)
        self.table_frame.pack(fill="both", expand=True)


        # ---------------- Run Query Controls ----------------
        self.run_frame = ctk.CTkFrame(self.output_frame, fg_color="transparent")
        self.run_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=10)

        self.run_btn = ctk.CTkButton(
            self.run_frame, text="▶ Run Query",
            command=self.on_run, fg_color="#1f6aa5", hover_color="#144870"
        )
        self.run_btn.pack(side="left", padx=10)

        self.timer_label = ctk.CTkLabel(self.run_frame, text="")
        self.timer_label.pack(side="left", padx=10)

        self.status_label = ctk.CTkLabel(
            self.run_frame, text="Device: N/A | User: N/A | Step: Minute"
        )
        self.status_label.pack(side="left", padx=10)

    def build_log(self):
        # Log collapsible goes in root row=2
        self.log_section = CollapsibleSection(self.root, title="Process Log")
        self.log_section.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)

        self.log_section.content.grid_rowconfigure(0, weight=1)
        self.log_section.content.grid_columnconfigure(0, weight=1)

        self.log_text = ctk.CTkTextbox(self.log_section.content, height=100)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    # -------------------------------
    # Table handling
    # -------------------------------
    def _clamp_view(self):
        # don’t allow overscroll (prevents “white” edges)
        MT = self.sheet.MT
        x0, x1 = MT.xview()
        y0, y1 = MT.yview()
        win_w = max(1e-9, x1 - x0)
        win_h = max(1e-9, y1 - y0)

        if x0 < 0.0:
            MT.xview_moveto(0.0)
        elif x1 > 1.0:
            MT.xview_moveto(max(0.0, 1.0 - win_w))

        if y0 < 0.0:
            MT.yview_moveto(0.0)
        elif y1 > 1.0:
            MT.yview_moveto(max(0.0, 1.0 - win_h))

    def _force_redraw(self):
        # Coalesce multiple requests into one after_idle call
        if getattr(self, "_frame_scheduled", False):
            return
        self._frame_scheduled = True

        def _do():
            try:
                self.sheet.refresh()
            except Exception:
                pass
            self._frame_scheduled = False

        self.root.after_idle(_do)


    def _ensure_at_least_one_column_selected(self):
        """Guarantee at least one visible column so the table never renders blank."""
        selected = [col for col, var in self.col_vars.items() if var.get()]
        if selected:
            return selected

        # Prefer updated_at if present, otherwise first column
        fallback = None
        if "updated_at" in self.col_vars:
            fallback = "updated_at"
        elif self.col_vars:
            fallback = next(iter(self.col_vars.keys()), None)

        if fallback is not None:
            self.col_vars[fallback].set(True)
            return [fallback]
        return []  # shouldn't happen, but safe

    def show_table(self, df, col_states=None):
        """Render the table with full data (no custom virtualization).
           If selected metrics are missing from df, show blank columns for them.
        """
        # Empty state: clear the table area and remove any existing Sheet
        if df is None or df.empty or "updated_at" not in df.columns:
            for widget in self.table_frame.winfo_children():
                widget.destroy()
            return

        # Normalize and keep a stable row index for reordering
        df = df.reset_index(drop=True)
        self.df = df

        # (Assumes build_column_checkboxes shows all metrics from color_map)
        self.build_column_checkboxes(df.columns, col_states)

        # Ensure at least one column is selected
        selected_cols = self._ensure_at_least_one_column_selected()

        # Keep a stable ordering vector for sorting / reordering
        import numpy as np
        self._table_order = np.arange(len(df), dtype=int)

        # Compose headers + data matrix, including blanks for missing columns
        present = [c for c in selected_cols if c in self.df.columns]
        missing = [c for c in selected_cols if c not in self.df.columns]

        if present:
            visible_idx = [self.df.columns.get_loc(c) for c in present]
            full_data = self.df.iloc[self._table_order, visible_idx].to_numpy(copy=False).tolist()
        else:
            # No present columns → create empty rows to match row count
            full_data = [[] for _ in range(len(self._table_order))]

        # Append blank cells for each missing column
        if missing:
            for r in full_data:
                r.extend([""] * len(missing))

        headers = present + missing
        self._cached_headers = headers

        # Create the sheet if needed
        if not hasattr(self, "sheet"):
            self.sheet = Sheet(self.table_frame, height=200)
            self._bind_sheet_nav_keys()
            self.sheet.enable_bindings((
                "single_select", "row_select", "column_select", "drag_select", "arrowkeys",
                "column_width_resize", "row_height_resize", "double_click_column_resize",
                "copy", "select_all"
            ))
            self.sheet.pack(fill="both", expand=True)
            # Double-click header to sort
            try:
                self.sheet.CH.bind("<Double-Button-1>", self._hdr_double_click)
            except Exception:
                pass

        # Preserve current widths across reloads
        widths = self._get_col_widths()

        # One-shot render (no yscroll handler)
        self.sheet.headers(self._cached_headers, redraw=False)
        self.sheet.set_sheet_data(full_data, redraw=False)
        self.sheet.refresh()

        if widths:
            self._set_col_widths(widths)

        self.log(f"✅ Table linked with {len(df)} rows and {len(headers)} columns ("
                 f"{len(present)} present, {len(missing)} missing).")

    def _get_col_widths(self):
        # derive widths from current col_positions (most robust across tksheet versions)
        try:
            pos = list(self.sheet.MT.col_positions)
            return [pos[i + 1] - pos[i] for i in range(len(pos) - 1)]
        except Exception:
            # fallback to per-column getter if available
            widths = []
            try:
                n = len(self.sheet.headers())
            except Exception:
                n = 0
            for i in range(n):
                try:
                    w = self.sheet.column_width(i)  # some versions support getter
                except Exception:
                    w = None
                widths.append(w)
            return widths

    def _set_col_widths(self, widths):
        # apply as many as we have columns for
        for i, w in enumerate(widths):
            if w is None:
                continue
            try:
                # most versions support setter like this
                self.sheet.column_width(i, w, redraw=False)
            except TypeError:
                try:
                    self.sheet.column_width(i, w)
                except Exception:
                    pass
        try:
            self.sheet.refresh()
        except Exception:
            pass

    def _hdr_double_click(self, event):
        # Only sort when double-clicking inside the header,
        # and NOT near a divider (so auto-resize still wins there).
        try:
            if self.sheet.identify_region(event) != "header":
                return
        except Exception:
            return

        if self._is_near_header_divider(event):
            # Let tksheet's "double_click_column_resize" do its thing
            return

        if self._is_near_header_divider(event):
            return
        # Reuse your sorter (decides column & toggles direction)
        self.sort_by_header_click(event)

    def _is_near_header_divider(self, event, eps=6):
        """Return True if pointer is within eps px of a column divider in the header."""
        try:
            # x in ColumnHeader *widget* coords
            ch = self.sheet.CH
            local_x = event.x_root - ch.winfo_rootx()
            # convert to canvas coords (accounts for horizontal scroll)
            cx = ch.canvasx(local_x)

            # Prefer CH.col_positions; fall back to MT.col_positions
            positions = getattr(ch, "col_positions", None)
            if not positions:
                positions = getattr(self.sheet.MT, "col_positions", None)
            if not positions:
                return False

            # Close to any divider?
            for p in positions:
                if abs(cx - p) <= eps:
                    return True
        except Exception:
            pass
        return False

    def _bind_sheet_nav_keys(self):
        w = self.sheet
        # Ctrl + arrows
        w.bind("<Control-Left>", self._nav_left_end)
        w.bind("<Control-Right>", self._nav_right_end)
        w.bind("<Control-Up>", self._nav_top_end)
        w.bind("<Control-Down>", self._nav_bottom_end)

        # (nice extras)
        w.bind("<Home>", self._nav_left_end)
        w.bind("<End>", self._nav_right_end)
        w.bind("<Control-Home>", self._nav_top_left)
        w.bind("<Control-End>", self._nav_bottom_right)

    def _sync_headers(self):
        """Keep column header (CH) and row index (RI) aligned with the main table (MT)."""
        try:
            x0, _ = self.sheet.MT.xview()
            self.sheet.CH.xview_moveto(x0)
        except Exception:
            pass
        try:
            y0, _ = self.sheet.MT.yview()
            self.sheet.RI.yview_moveto(y0)
        except Exception:
            pass

    def _get_shape(self):
        """Shape of the logical table (selected rows, selected columns)."""
        try:
            nrows = len(self._table_order) if getattr(self, "_table_order", None) is not None else (
                len(self.df) if self.df is not None else 0)
            ncols = sum(1 for col, var in self.col_vars.items() if var.get())
        except Exception:
            nrows, ncols = 0, 0
        return nrows, ncols

    # ---------- NAV + VIEW HELPERS (add to class) ----------
    def _visible_shape(self):
        nrows = len(self._table_order) if getattr(self, "_table_order", None) is not None else (
            len(self.df) if self.df is not None else 0)
        ncols = len(self._cached_headers) if hasattr(self, "_cached_headers") and self._cached_headers else 0
        return nrows, ncols

    def _get_current_cell(self):
        try:
            cur = self.sheet.get_currently_selected()
            if isinstance(cur, tuple) and len(cur) >= 2:
                # protect against None
                r = int(cur[0]) if cur[0] is not None else 0
                c = int(cur[1]) if cur[1] is not None else 0
                return r, c
        except Exception:
            pass
        return 0, 0

    def _set_focus(self, r, c):
        r = max(0, r);
        c = max(0, c)
        try:
            self.sheet.set_currently_selected(r, c)
            # if available in your tksheet version:
            try:
                self.sheet.see(r, c)
            except Exception:
                pass
        except Exception:
            pass
        self._force_redraw()

    def _goto_x(self, frac: float):
        MT = self.sheet.MT
        x0, x1 = MT.xview()
        win_w = max(1e-9, x1 - x0)
        left = max(0.0, min(1.0 - win_w, frac))
        MT.xview_moveto(left)
        self._sync_headers()
        self._force_redraw()

    def _goto_y(self, frac: float):
        MT = self.sheet.MT
        y0, y1 = MT.yview()
        win_h = max(1e-9, y1 - y0)
        top = max(0.0, min(1.0 - win_h, frac))
        MT.yview_moveto(top)
        self._sync_headers()
        self._force_redraw()

    # ----- Ctrl+Arrow / Home/End handlers (called by _bind_sheet_nav_keys) -----
    def _nav_left_end(self, event=None):
        self._goto_x(0.0)
        r, _ = self._get_current_cell()
        self._set_focus(r, 0)
        return "break"

    def _nav_right_end(self, event=None):
        # only scroll if there is room to scroll
        try:
            x0, x1 = self.sheet.MT.xview()
            if (x1 - x0) < 0.999:  # there is horizontal scrollable space
                self._goto_x(1.0)
        except Exception:
            pass

        r, _ = self._get_current_cell()
        _, ncols = self._get_shape()  # <-- now visible column count
        self._set_focus(r, max(0, ncols - 1))  # last visible column
        return "break"

    def _nav_top_end(self, event=None):
        self._goto_y(0.0)
        _, c = self._get_current_cell()
        self._set_focus(0, c)
        return "break"

    def _nav_bottom_end(self, event=None):
        self._goto_y(1.0)
        _, c = self._get_current_cell()
        nrows, _ = self._get_shape()
        self._set_focus(max(0, nrows - 1), c)
        return "break"

    def _nav_top_left(self, event=None):
        self._goto_y(0.0);
        self._goto_x(0.0)
        self._set_focus(0, 0)
        return "break"

    def _nav_bottom_right(self, event=None):
        self._goto_y(1.0);
        self._goto_x(1.0)
        nrows, ncols = self._visible_shape()
        self._set_focus(max(0, nrows - 1), max(0, ncols - 1))
        return "break"

    def build_column_checkboxes(self, columns, col_states=None):
        # Clear old
        for widget in self.metrics_col_frame.winfo_children():
            widget.destroy()
        for widget in self.other_col_frame.winfo_children():
            widget.destroy()

        self.col_vars = {}
        max_rows = 3

        # ✅ Always show metrics from the color_map, even if not in df
        metrics = list(self.color_map.keys())

        # Others = whatever the DF actually has besides metrics
        others = [col for col in columns if col not in self.color_map]

        def update_select_all_states():
            if metrics:
                self.metrics_toggle.set(all(self.col_vars[c].get() for c in metrics if c in self.col_vars))
            else:
                self.metrics_toggle.set(False)
            if others:
                self.other_toggle.set(all(self.col_vars[c].get() for c in others if c in self.col_vars))
            else:
                self.other_toggle.set(False)

        def per_box_cmd():
            self.on_column_change()
            update_select_all_states()

        # --- Metrics checkboxes (always) ---
        for i, col in enumerate(metrics):
            default_val = col_states.get(col, True) if col_states else True
            var = ctk.BooleanVar(value=default_val)
            chk = ctk.CTkCheckBox(
                self.metrics_col_frame,
                text=col,
                variable=var,
                command=per_box_cmd,
                fg_color=self.color_map[col],
                text_color=self.color_map[col],
            )
            chk.grid(row=i % max_rows, column=i // max_rows, padx=5, pady=5, sticky="w")
            self.col_vars[col] = var

        if metrics:
            self.metrics_toggle.set(all(self.col_vars[c].get() for c in metrics))
            row = len(metrics) % max_rows
            col = len(metrics) // max_rows
            ctk.CTkCheckBox(
                self.metrics_col_frame,
                text="Select All",
                variable=self.metrics_toggle,
                command=lambda: (self.toggle_metrics(), update_select_all_states()),
            ).grid(row=row, column=col, padx=5, pady=5, sticky="w")

        # --- Other checkboxes (only those present in df) ---
        for i, col in enumerate(others):
            default_val = col_states.get(col, True) if col_states else True
            var = ctk.BooleanVar(value=default_val)
            chk = ctk.CTkCheckBox(
                self.other_col_frame,
                text=col,
                variable=var,
                command=per_box_cmd,
            )
            chk.grid(row=i % max_rows, column=i // max_rows, padx=5, pady=5, sticky="w")
            self.col_vars[col] = var

        if others:
            self.other_toggle.set(all(self.col_vars[c].get() for c in others))
            row = len(others) % max_rows
            col = len(others) // max_rows
            ctk.CTkCheckBox(
                self.other_col_frame,
                text="Select All",
                variable=self.other_toggle,
                command=lambda: (self.toggle_others(), update_select_all_states()),
            ).grid(row=row, column=col, padx=5, pady=5, sticky="w")

    def _compose_table_matrix(self, selected_cols):
        """Return (headers, data) including blank columns for metrics not in df."""
        if self.df is None:
            return [], []

        present = [c for c in selected_cols if c in self.df.columns]
        missing = [c for c in selected_cols if c not in self.df.columns]

        # Build data for present columns
        import numpy as np
        idxs = [self.df.columns.get_loc(c) for c in present]
        arr = self.df.iloc[self._table_order, idxs].to_numpy(copy=False) if present else np.empty(
            (len(self._table_order), 0))
        data = arr.tolist()

        # Append blanks for missing columns
        if missing:
            for r in data:
                r.extend([""] * len(missing))

        headers = present + missing
        return headers, data

    # -------------------------------
    # Cache + log + timer helpers (unchanged from before)
    # -------------------------------

    def log(self, message: str):
        if self.is_closing:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")

        def _append():
            try:
                self.log_text.insert("end", f"[{timestamp}] {message}\n")
                self.log_text.see("end")
                # remove update_idletasks() — it can cause re-entrancy hiccups
            except Exception:
                pass

        # Only touch Tk on the main thread
        if threading.current_thread() is threading.main_thread():
            _append()
        else:
            self.safe_after(0, _append)

    def start_timer(self):
        self.query_start_time = time.time()
        self.timer_running = True
        self.update_timer()

    def update_timer(self):
        if self.is_closing or not self.timer_running:
            return
        elapsed = time.time() - self.query_start_time
        self.timer_label.configure(text=f"⏱ Elapsed: {elapsed:.1f}s")
        # keep the id so we can cancel *just* this loop
        self._timer_after_id = self.root.after(100, self.update_timer)

    def stop_timer(self):
        self.timer_running = False
        if self._timer_after_id:
            try:
                self.root.after_cancel(self._timer_after_id)
            except Exception:
                pass
            self._timer_after_id = None

    def safe_after(self, delay, func, *args, **kwargs):
        if self.is_closing:
            return
        try:
            after_id = self.root.after(delay, func, *args, **kwargs)
            self.after_ids.append(after_id)
            return after_id
        except Exception as e:
            self.log(f"[AFTER] Failed to schedule: {e}")
            return

    def cancel_afters(self):
        for aid in self.after_ids:
            try:
                self.root.after_cancel(aid)
            except Exception as e:
                self.log(f"[AFTER] Failed to cancel id={aid}: {e}")
        self.after_ids.clear()

    def sort_by_header_click(self, event):
        # Only header clicks
        try:
            if self.sheet.identify_region(event) != "header":
                return
            vis_col = self.sheet.identify_column(event, exclude_header=False)
        except Exception:
            return

        if vis_col is None or self.df is None or self.df.empty:
            return

        # Map visible column index -> real df column index
        visible_cols = [i for i, col in enumerate(self.df.columns) if self.col_vars[col].get()]
        if not (0 <= vis_col < len(visible_cols)):
            return

        col_idx = visible_cols[vis_col]
        col_name = self.df.columns[col_idx]

        # Toggle direction per column
        asc = self._sort_dir.get(col_name, True)
        self._sort_dir[col_name] = not asc

        # Remember scroll
        MT = self.sheet.MT
        try:
            x0, _ = MT.xview()
            y0, _ = MT.yview()
        except Exception:
            x0 = y0 = 0.0

        import threading
        import pandas as pd
        import numpy as np

        def keynorm(v):
            # Place NaNs at bottom; compare numbers as numbers, others by casefolded str
            if pd.isna(v):
                return (1, 0, "")
            if isinstance(v, (int, float, np.number)):
                return (0, 0, float(v))
            return (0, 1, str(v).casefold())

        def worker():
            order_idx = list(range(len(self._table_order)))
            try:
                series = self.df.iloc[self._table_order, col_idx]
                order_idx.sort(key=lambda i: keynorm(series.iat[i]), reverse=not asc)
                new_order = [self._table_order[i] for i in order_idx]
            except Exception:
                # Fallback: safe but slower path
                series = [self.df.iat[r, col_idx] for r in self._table_order]
                order_idx.sort(key=lambda i: keynorm(series[i]), reverse=not asc)
                new_order = [self._table_order[i] for i in order_idx]

            def apply():
                self._apply_row_order(new_order)
                try:
                    MT.xview_moveto(x0)
                    MT.yview_moveto(y0)
                    self._sync_headers()
                except Exception:
                    pass
                self.log(f'Sorted by “{col_name}” ({"asc" if asc else "desc"})')

            self.safe_after(0, apply)

        threading.Thread(target=worker, daemon=True).start()

    def get_selected_metrics(self):
        """Columns that are both checked and in color_map (for plotting + tooltips)."""
        return [col for col, var in self.col_vars.items() if var.get() and col in self.color_map]

    def get_selected_table_columns(self):
        """Columns that are checked, regardless of whether they're metrics or other."""
        return [col for col, var in self.col_vars.items() if var.get()]

    def _to_canvas_xy(self, canvas, event):
        # translate event coords (from self.sheet) to the target canvas' coords
        return (event.x_root - canvas.winfo_rootx(),
                event.y_root - canvas.winfo_rooty())

    def on_table_pan_start(self, event):
        self.table_panning = True
        mx, my = self._to_canvas_xy(self.sheet.MT, event)
        self.sheet.MT.scan_mark(mx, my)
        self._last_scan_xy = (mx, my)  # ✅ reset tracking

    def on_table_pan_drag(self, event):
        if not self.table_panning:
            return
        MT = self.sheet.MT
        mx, my = self._to_canvas_xy(MT, event)

        # edge guards (keep yours if you like)
        x0, x1 = MT.xview()
        y0, y1 = MT.yview()
        prev = getattr(self, "_last_scan_xy", (mx, my))
        dx, dy = (mx - prev[0], my - prev[1])
        self._last_scan_xy = (mx, my)
        if (x0 <= 1e-6 and dx > 0) or (y0 <= 1e-6 and dy > 0):
            return

        MT.scan_dragto(mx, my, gain=1)
        self._sync_headers()
        # Don't clamp + repaint twice; let a single coalesced refresh run:
        self._force_redraw()

    def on_table_pan_stop(self, event):
        self.table_panning = False

    def _pan_refresh_tick(self):
        if not getattr(self, "table_panning", False):
            return
        try:
            self.sheet.refresh()
        except Exception:
            pass
        self._pan_refresh_job = self.root.after(33, self._pan_refresh_tick)

    # -------------------------------
    # Events
    # -------------------------------
    def on_run(self):
        if not self.filter_value.get().strip():
            messagebox.showerror("Error", "Filter Value cannot be empty.")
            return
        self.start_timer()
        t = threading.Thread(target=self._run_query_worker, daemon=True)
        self.threads.append(t)
        t.start()

    def _update_select_all_checks(self):
        # Metrics group
        metrics_cols = [c for c in self.col_vars if c in self.color_map]
        if metrics_cols:
            all_metrics_checked = all(self.col_vars[c].get() for c in metrics_cols)
            # If you uncheck any metric, this will flip "Select All" off
            self.metrics_toggle.set(all_metrics_checked)
        else:
            self.metrics_toggle.set(False)

        # Others group
        other_cols = [c for c in self.col_vars if c not in self.color_map]
        if other_cols:
            all_others_checked = all(self.col_vars[c].get() for c in other_cols)
            self.other_toggle.set(all_others_checked)
        else:
            self.other_toggle.set(False)

    def _run_query_worker(self):
        try:
            if not self.query_manager.connect():
                self.log("⏹ Query aborted (no DB connection).")
                return  # stop cleanly if cancelled

            # If no checkboxes yet (first run / no cache), request ALL columns
            sel_cols = self.get_selected_table_columns()
            if not sel_cols:
                sel_cols = None  # None = ask QueryManager for all columns

            try:
                start, end = self._get_validated_date_range()
            except ValueError as e:
                self.safe_after(0, messagebox.showerror, "Invalid Dates", str(e))
                return

            df = self.query_manager.run_query(
                filter_type=self.filter_type.get(),
                filter_value=self.filter_value.get(),
                start_date_str=start.strftime("%Y-%m-%d"),
                end_date_str=end.strftime("%Y-%m-%d"),
                selected_columns=sel_cols
            )

            self.df = df
            if df.empty:
                self.log("⚠️ No metrics available for the given filter/date range.")
            else:
                self.log(f"Query complete. Retrieved {len(df)} rows.")

            def _render_first_time():
                # ✅ Always build checkboxes for both table & plot
                self.build_column_checkboxes(df.columns, getattr(self, "_saved_col_states", None))
                self._saved_col_states = None

                if self.enable_table:
                    self.show_table(df)

                if self.enable_plot:
                    metrics = self.get_selected_metrics()
                    sel = metrics if metrics else None
                    self.plot_manager.plot_data(df, sel, True, self.color_map)

                    # Force the x-axis to the exact LA calendar window from the entries
                    s = datetime.fromisoformat(self.start_date_entry.get()).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    e = datetime.fromisoformat(self.end_date_entry.get()).replace(
                        hour=23, minute=59, second=59, microsecond=999999
                    )
                    self.plot_manager.set_time_window(s, e)

            # Schedule rendering on the main thread
            self.safe_after(0, _render_first_time)

            if not df.empty:
                device_val = df["device_name"].dropna().iloc[0] if "device_name" in df else self.filter_value.get()
                user_val = df["user_id"].dropna().iloc[0] if "user_id" in df else "?"
                self.safe_after(
                    0,
                    self.status_label.configure,
                    {"text": f"Device: {device_val} | User: {user_val} | Step: Minute"}
                )

        except Exception as e:
            if not self.is_closing:
                self.log(f"❌ Error: {e}")
                self.safe_after(0, messagebox.showerror, "Error", str(e))

        finally:
            self.safe_after(0, self.stop_timer)

    def on_column_change(self):
        if self.df is not None:
            if self.enable_plot:
                self.plot_manager.plot_data(
                    self.df, self.get_selected_metrics(), fresh=False, color_map=self.color_map
                )
            if self.enable_table:
                self.update_table_columns()
            ...

            # keep "Select All" boxes in sync with individual boxes
            self._update_select_all_checks()

    def update_table_columns(self):
        if not hasattr(self, "sheet") or self.df is None:
            return
        widths = self._get_col_widths()
        selected_cols = self._ensure_at_least_one_column_selected()
        self._cached_headers, full_data = self._compose_table_matrix(selected_cols)
        self.sheet.headers(self._cached_headers, redraw=False)
        self.sheet.set_sheet_data(full_data, redraw=False)
        self.sheet.refresh()

        # Only reapply widths if shape didn’t change
        if widths and len(widths) == len(self._cached_headers):
            self._set_col_widths(widths)

    def _apply_row_order(self, new_order):
        if self.df is None or not hasattr(self, "sheet"):
            return
        import numpy as np
        self._table_order = np.array(new_order, dtype=int)
        widths = self._get_col_widths()
        selected_cols = self._ensure_at_least_one_column_selected()
        self._cached_headers, full_data = self._compose_table_matrix(selected_cols)
        self.sheet.headers(self._cached_headers, redraw=False)
        self.sheet.set_sheet_data(full_data, redraw=False)
        self.sheet.refresh()
        if widths:
            self._set_col_widths(widths)

    def on_select(self, eclick, erelease):
        pass

    def on_key(self, event):
        pass

    def on_close(self):
        if self.is_closing:
            return
        self.is_closing = True
        self.log("[CLOSE] Starting shutdown sequence")

        # (optional) disable UI so nothing else fires
        try:
            self.run_btn.configure(state="disabled")
        except Exception:
            pass

        # Save config (unchanged)
        try:
            state = self.root.state()
            cfg = {
                "filter_type": self.filter_type.get(),
                "filter_value": self.filter_value.get(),
                "start_date": self.start_date_entry.get(),
                "end_date": self.end_date_entry.get(),
                "columns": self.get_selected_table_columns(),
                "col_states": {col: var.get() for col, var in self.col_vars.items()},
                "window_state": state,
            }
            if state == "normal":
                cfg["window_size"] = self.root.geometry()
            cfg["collapsible_states"] = {
                "filter_date": self.filter_date_section.get_state(),
                "table": self.table_section.get_state(),
                "log": self.log_section.get_state(),
            }
            config_manager.save_config(cfg)
            self.log("[CLOSE] Saved config")
        except Exception as e:
            self.log(f"[CLOSE] Failed to save config: {e}")

        # hard-stop the timer loop and ALL pending after jobs
        self._cancel_all_afters_shutdown()

        # let worker threads die on their own (daemon=True), don’t join
        for t in self.threads:
            if t.is_alive():
                self.log(f"[CLOSE] Thread {t.name} still alive, skipping join")

        # close fast without giving Tk time to run more after callbacks
        try:
            self.root.withdraw()
        except Exception:
            pass
        try:
            self.root.quit()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass

    def run(self):
        self.root.mainloop()

